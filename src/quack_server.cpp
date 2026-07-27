#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "duckdb/main/client_config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_random.hpp"
#include "quack_result_cache.hpp"
#include "quack_storage.hpp"
#include "quack_session_state.hpp"
#include "quack_fetch_collector.hpp"
#include "quack_rebalancer_sink.hpp"

#include "mbedtls_wrapper.hpp"

namespace duckdb {

static bool LeaseTimeoutElapsed(time_point<steady_clock> last_renewed_at, time_point<steady_clock> now,
                                idx_t timeout_seconds) {
	if (now <= last_renewed_at) {
		return false;
	}
	auto elapsed_seconds = duration_cast<std::chrono::seconds>(now - last_renewed_at).count();
	return static_cast<idx_t>(elapsed_seconds) >= timeout_seconds;
}

QuackConnection::QuackConnection(string session_id_p, idx_t heartbeat_timeout_seconds_p)
    : session_id(std::move(session_id_p)), heartbeat_timeout_seconds(heartbeat_timeout_seconds_p),
      lease_last_renewed_at(steady_clock::now()) {
}

bool QuackConnection::LeaseExpiredLocked(time_point<steady_clock> now) {
	if (!lease_expired && LeaseTimeoutElapsed(lease_last_renewed_at, now, heartbeat_timeout_seconds)) {
		lease_expired = true;
	}
	return lease_expired;
}

bool QuackConnection::TryRenewLease() {
	annotated_lock_guard<annotated_mutex> guard(lease_lock);
	auto now = steady_clock::now();
	if (LeaseExpiredLocked(now)) {
		return false;
	}
	lease_last_renewed_at = now;
	return true;
}

//! A fetch stream and its producer thread, taken off the connection so the join can run unlocked.
struct DetachedResultStream {
	shared_ptr<QuackResultStream> stream;
	std::thread thread;
};

//! Error the buffer, so consumers and parked producers wake. If this is the statement that runs right
//! now, interrupt it and error the client streams its scan may wait on. The caller holds
//! statements_lock, so the running statement cannot change underneath: one that has not published
//! itself yet finds the error when it does, and never starts.
static void FailStatementLocked(QuackConnection &connection, QuackResultStream &stream, const string &reason) {
	auto was_finished = stream.buffer.Finished();
	stream.buffer.SetError(ErrorData(ExceptionType::INTERRUPT, reason));
	if (was_finished || connection.running_stream.get() != &stream || !connection.duckdb_connection) {
		return;
	}
	if (auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context)) {
		session_state->Streams().Clear(reason);
	}
	connection.duckdb_connection->Interrupt();
}

//! Take the statement `uuid` off the connection, failing its stream. The entry stays, with its error,
//! so a late FETCH learns why the result is gone; `erase` drops it outright. Caller holds
//! statements_lock; the returned thread is joined WITHOUT it.
static DetachedResultStream DetachStatementLocked(QuackConnection &connection, hugeint_t uuid, const string &reason,
                                                  bool erase) {
	DetachedResultStream detached;
	auto entry = connection.statements.find(uuid);
	if (entry == connection.statements.end()) {
		return detached;
	}
	auto &state = *entry->second;
	detached.stream = std::move(state.stream);
	detached.thread = std::move(state.thread);
	if (detached.stream) {
		// the abort reason must not hide a real failure
		state.abort_error = detached.stream->buffer.HasError() ? detached.stream->buffer.GetError()
		                                                       : ErrorData(ExceptionType::INTERRUPT, reason);
		FailStatementLocked(connection, *detached.stream, reason);
	}
	state.exhausted_at = connection.prepare_count;
	if (erase) {
		connection.statements.erase(entry);
	}
	return detached;
}

//! Join the producer. Call this WITHOUT the statement locks: a producer that still runs holds one.
static void JoinDetachedStatement(DetachedResultStream &detached) {
	if (detached.thread.joinable()) {
		detached.thread.join();
	}
}

//! Stop and join the statement `uuid`. Without `erase` the entry keeps the error for a late FETCH.
static void AbortStatement(QuackConnection &connection, hugeint_t uuid, const string &reason, bool erase = false) {
	DetachedResultStream detached;
	{
		lock_guard<mutex> guard(connection.statements_lock);
		detached = DetachStatementLocked(connection, uuid, reason, erase);
	}
	JoinDetachedStatement(detached);
}

//! Stop and join every statement on the connection (cancellation, teardown).
static void AbortAllStatements(QuackConnection &connection, const string &reason) {
	vector<DetachedResultStream> detached;
	{
		lock_guard<mutex> guard(connection.statements_lock);
		for (auto &entry : connection.statements) {
			detached.push_back(DetachStatementLocked(connection, entry.first, reason, false));
		}
		connection.statements.clear();
	}
	for (auto &entry : detached) {
		JoinDetachedStatement(entry);
	}
}

//! Note that the client has seen the end of result `uuid`, so its release can be scheduled
static void MarkExhausted(QuackConnection &connection, hugeint_t uuid) {
	lock_guard<mutex> guard(connection.statements_lock);
	auto entry = connection.statements.find(uuid);
	if (entry != connection.statements.end() && !entry->second->exhausted_at.IsValid()) {
		entry->second->exhausted_at = connection.prepare_count;
	}
}

//! Drop the replay cache. Its stream may still serve a scan, so the stream only stops retaining from
//! here on; the acks release what it holds. Caller holds connection.lock.
static void DisplaceResultCache(QuackConnection &connection) {
	if (connection.result_cache && connection.result_cache->stream) {
		connection.result_cache->stream->StopRetaining();
	}
	connection.ClearResultCache();
}

//! Update the cache with what its stream holds. Drops the cache when the result grows past
//! quack_cache_max_rows, because it is then too large to keep for a reconnect.
static void SyncResultCache(QuackConnection &connection, QuackResultStream &stream, DatabaseInstance &db) {
	std::unique_lock<std::mutex> lock(connection.lock);
	auto &cache = connection.result_cache;
	if (!cache || cache->stream.get() != &stream) {
		return;
	}
	cache->last_served_at = Timestamp::GetCurrentTimestamp();
	cache->retained_rows = stream.RetainedRows();
	auto max_cache_rows = CacheMaxRows(db);
	if (max_cache_rows != 0 && cache->retained_rows > max_cache_rows) {
		// the acks release the payloads again
		stream.DropRetention();
		connection.ClearResultCache();
		return;
	}
	connection.SyncCachedRows();
}

//! Runs one client statement with the result collector installed. The caller holds the statement lock. A
//! statement that returns rows fills the claim buffer in parallel. A statement that returns a count
//! leaves the count in batch 1, through the fallback below.
static void RunStatement(QuackConnection &connection, const shared_ptr<QuackResultStream> &stream, const string &sql) {
	// A failed statement must stop the client's sends, so its scan's streams get the error too.
	auto fail_streams = [&](const ErrorData &error) {
		if (auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context)) {
			session_state->Streams().Fail(error);
		}
	};
	try {
		auto &context = *connection.duckdb_connection->context;
		if (auto session_state = QuackSessionState::Get(context)) {
			// The previous statement's client streams are done with. This statement registers its own
			// while it binds, and a scan that does so raises `stream`, so its PREPARE stops waiting.
			session_state->Streams().Clear();
			session_state->SetStatement(stream);
		}

		// MakeQuackFetchCollector sends the FIRST statement that returns a result into the stream.
		// Every other statement keeps the default collector.
		auto &config = ClientConfig::GetConfig(context);
		config.get_result_collector = [stream](ClientContext &ctx, PreparedStatementData &data) {
			return MakeQuackFetchCollector(ctx, data, stream);
		};
		unique_ptr<QueryResult> result;
		try {
			result = connection.duckdb_connection->Query(sql);
		} catch (...) {
			// leave no collector hook on the connection's config
			config.get_result_collector = nullptr;
			throw;
		}
		config.get_result_collector = nullptr;
		if (result->HasError()) {
			stream->buffer.SetError(result->GetErrorObject());
			fail_streams(result->GetErrorObject());
		} else if (!stream->Bound()) {
			// No collector claimed the stream, because no statement returned a result. Send the last
			// statement's Success/Count result, as the protocol always has. No columns is an error.
			if (result->GetNames().empty()) {
				stream->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
				return;
			}
			// BaseQueryResult::names is a vector<Identifier>. The stream carries plain strings.
			vector<string> result_names;
			result_names.reserve(result->GetNames().size());
			for (auto &col_name : result->GetNames()) {
				result_names.push_back(col_name.GetIdentifierName());
			}
			stream->SignalBound(result->GetTypes(), std::move(result_names));
			unique_ptr<QuackChunkPayloadWriter> writer;
			idx_t rows = 0;
			while (auto chunk = result->Fetch()) {
				if (chunk->size() == 0) {
					continue;
				}
				if (!writer) {
					writer = make_uniq<QuackChunkPayloadWriter>(0);
				}
				writer->AppendChunk(*chunk);
				rows += chunk->size();
			}
			if (writer) {
				auto sealed = writer->Seal();
				QuackFetchPayload entry;
				entry.payload = std::move(sealed.payload);
				entry.payload_size = sealed.payload_size;
				entry.chunk_count = sealed.chunk_count;
				entry.rows = rows;
				auto bytes = entry.payload_size;
				stream->buffer.PushBatch(1, std::move(entry), bytes);
				stream->announced_total = 1;
			} else {
				stream->announced_total = 0;
			}
		}
	} catch (std::exception &ex) {
		stream->buffer.SetError(ErrorData(ex));
		fail_streams(ErrorData(ex));
	}
}

//! The producer thread of one PREPAREd statement.
static void DriveQuery(QuackConnection &connection, shared_ptr<QuackResultStream> stream, string sql) {
	{
		// Held for the whole statement: the DuckDB connection runs one at a time, so a statement PREPAREd
		// while another one still produces queues here until it has finished.
		unique_lock<mutex> guard(connection.statement_lock);
		bool run;
		{
			lock_guard<mutex> statements_guard(connection.statements_lock);
			// aborted while it queued: there is nothing left to run
			run = !stream->buffer.HasError();
			if (run) {
				connection.running_stream = stream;
			}
		}
		if (run) {
			RunStatement(connection, stream, sql);
			lock_guard<mutex> statements_guard(connection.statements_lock);
			connection.running_stream.reset();
		}
	}
	// Close against the announced total, so a short stream errors instead of truncating.
	stream->buffer.Finish(stream->announced_total);
}

QuackConnection::~QuackConnection() {
	// Abort and join the query drivers before the members go away.
	AbortAllStatements(*this, "connection closed during fetch");
}

void QuackServer::ValidateToken(const string &token) {
	if (token.size() < 4) {
		throw InvalidInputException("Quack server token must be at least 4 characters long");
	}
}

QuackServer::QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : db_ptr(context_p.db), uri(uri_p), token(token_p) {
	ValidateToken(token);
	server_hmac_key = QuackRandomToken(*context_p.db);
}

QuackServer::~QuackServer() {
}

void QuackServer::RecordRequest(int connection_key) {
	// httplib hands a keep-alive connection to one worker thread and keeps it there until the
	// connection closes, so a key that differs from the last one seen on this thread means a
	// fresh connection was accepted. A new connection that happens to reuse the previous
	// connection's ephemeral port *and* lands on the same worker is undercounted; that is
	// acceptable for a reuse indicator, and costs nothing to maintain.
	static thread_local int last_connection_key = -1;
	if (connection_key != last_connection_key) {
		last_connection_key = connection_key;
		client_connection_count++;
	}
	request_count++;
}

void QuackServer::CleanupExpiredConnection(QuackConnection &connection) {
	if (connection.duckdb_connection) {
		connection.duckdb_connection->Interrupt();
	}
	// Interrupt() cannot wake a producer that is parked on the buffer capacity. Abort and join it
	// here, not in ~QuackConnection, so the connection is quiet before the handler continues.
	AbortAllStatements(connection, "connection heartbeat lease expired");
}

bool QuackServer::RenewConnectionLease(const string &connection_id, const shared_ptr<QuackConnection> &connection) {
	shared_ptr<QuackConnection> expired_connection;
	{
		std::lock_guard<std::mutex> guard(active_connections_mutex);
		auto entry = active_connections.find(connection_id);
		if (entry == active_connections.end() || entry->second.get() != connection.get()) {
			return false;
		}
		if (connection->TryRenewLease()) {
			return true;
		}
		expired_connection = std::move(entry->second);
		active_connections.erase(entry);
	}
	CleanupExpiredConnection(*expired_connection);
	return false;
}

void QuackServer::DestroyCachesDetached(vector<unique_ptr<QuackResultCache>> doomed) {
	if (doomed.empty()) {
		return;
	}
	auto db = db_ptr.lock();
	if (!db) {
		// database teardown, destroy inline as this scope ends
		return;
	}
	// the thread owns only the doomed caches (plus the db keeping their buffers valid), never the server
	std::thread([db = std::move(db), doomed = std::move(doomed)] {}).detach();
}

vector<QuackConnectionSnapshot> QuackServer::GetActiveConnectionSnap() {
	// sql_query and query_started_at are written under QuackConnection::lock, so reading them under
	// active_connections_mutex alone is a data race — and sql_query is a std::string, so a concurrent
	// write can free the storage we copy. (query_state is atomic and would be safe unlocked, but we
	// read it in the same critical section for a snapshot consistent with the other two.) Copy the
	// connection handles out under active_connections_mutex, release it, then read each connection's
	// fields under its own lock (never holding both mutex classes at once).
	vector<shared_ptr<QuackConnection>> connections;
	{
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		connections.reserve(active_connections.size());
		for (auto &[id, conn] : active_connections) {
			connections.push_back(conn);
		}
	}
	vector<QuackConnectionSnapshot> result;
	result.reserve(connections.size());
	for (auto &conn : connections) {
		QuackConnectionSnapshot snapshot;
		// session_id and client_id_hash are set once when the connection is created, before it is
		// published to active_connections, and never rewritten — so they need no lock here.
		snapshot.session_id = conn->session_id;
		snapshot.client_id_hash = conn->client_id_hash;
		{
			std::lock_guard<std::mutex> lock(conn->lock);
			snapshot.sql_query = conn->sql_query;
			snapshot.query_state = conn->query_state;
			snapshot.query_started_at = conn->query_started_at;
		}
		auto cached_rows = conn->cached_rows.load();
		if (cached_rows != DConstants::INVALID_INDEX) {
			snapshot.cached_rows = cached_rows;
		}
		result.push_back(std::move(snapshot));
	}
	return result;
}

void QuackServer::RegisterCacheForExpiry(QuackConnection &connection) {
	if (connection.cache_in_expiry_queue) {
		// the sweep re-queues the existing slot under the new cache's timestamp once its old snapshot pops
		return;
	}
	connection.cache_in_expiry_queue = true;
	std::lock_guard<std::mutex> lock(cache_expiry_mutex);
	cache_expiry_queue.push({connection.result_cache->last_served_at, connection.session_id});
}

void QuackServer::SweepExpiredCaches(DatabaseInstance &db) {
	// nothing cached anywhere, we skip the settings lookup and the expiry queue
	if (live_caches->load(std::memory_order_relaxed) == 0) {
		return;
	}
	auto ttl_micros = ResultTtlMicros(db);
	if (ttl_micros == 0) {
		return;
	}
	auto now = Timestamp::GetCurrentTimestamp();
	vector<CacheExpiryEntry> candidates;
	{
		std::lock_guard<std::mutex> lock(cache_expiry_mutex);
		while (!cache_expiry_queue.empty() && now.value - cache_expiry_queue.top().served_at.value > ttl_micros) {
			candidates.push_back(cache_expiry_queue.top());
			cache_expiry_queue.pop();
		}
	}
	vector<CacheExpiryEntry> requeue;
	vector<unique_ptr<QuackResultCache>> doomed;
	// Their producer must be aborted, which joins a thread. Do that after the lock is released.
	struct AbandonedResult {
		shared_ptr<QuackConnection> connection;
		hugeint_t uuid;
	};
	vector<AbandonedResult> abandoned;
	for (auto &entry : candidates) {
		auto connection = GetConnection(entry.session_id);
		if (!connection) {
			// disconnected, the cache died with the connection and the slot dies here
			continue;
		}
		{
			std::unique_lock<std::mutex> lock(connection->lock, std::try_to_lock);
			if (!lock.owns_lock()) {
				// another handler is mutating the session state; retry on the next sweep
				requeue.push_back(std::move(entry));
				continue;
			}
			bool still_serving = false;
			auto expired = ExpireCacheIfStale(*connection, now, ttl_micros, still_serving);
			if (expired) {
				if (still_serving) {
					abandoned.push_back({connection, expired->query_uuid});
				}
				doomed.push_back(std::move(expired));
			}
			if (connection->result_cache) {
				// served (or replaced) since the snapshot was queued, keep the slot under the fresh timestamp
				entry.served_at = connection->result_cache->last_served_at;
				requeue.push_back(std::move(entry));
			} else {
				connection->cache_in_expiry_queue = false;
			}
		}
	}
	for (auto &entry : abandoned) {
		// Releases the query the client walked away from. A later FETCH finds this error.
		AbortStatement(*entry.connection, entry.uuid, "Query was interrupted");
	}
	if (!requeue.empty()) {
		std::lock_guard<std::mutex> lock(cache_expiry_mutex);
		for (auto &entry : requeue) {
			cache_expiry_queue.push(std::move(entry));
		}
	}
	DestroyCachesDetached(std::move(doomed));
}

shared_ptr<QuackConnection> QuackServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second;
	}
	return nullptr;
}

string QuackServer::CreateNewConnection(const string &session_id, const string &client_id_hash,
                                        idx_t heartbeat_timeout_seconds) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto new_connection = make_shared_ptr<QuackConnection>(session_id, heartbeat_timeout_seconds);
	new_connection->client_id_hash = client_id_hash;
	new_connection->live_caches = live_caches;
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	auto &connection_context = *new_connection->duckdb_connection->context;
	// scan_data_from_quack_client registers its streams here, and SEND_DATA finds them by connection.
	connection_context.registered_state->Insert(QuackSessionState::KEY, make_shared_ptr<QuackSessionState>(session_id));
	connection_context.config.enable_progress_bar = false;
	// new_connection->duckdb_connection->context->config.streaming_buffer_size = 10 * 1000000; // 10 MB
	active_connections[session_id] = std::move(new_connection);
	return session_id;
}

bool QuackServer::DisconnectConnection(const string &session_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	auto entry = active_connections.find(session_id);
	if (entry == active_connections.end()) {
		// unknown client
		return false;
	}
	active_connections.erase(entry);
	return true;
}

static string GetSettingString(DatabaseInstance &db, const string &setting_name) {
	Value setting_val;
	auto &config = DBConfig::GetConfig(db);

	auto lookup_result = config.TryGetCurrentSetting(Identifier(setting_name), setting_val);
	D_ASSERT(lookup_result);
	D_ASSERT(setting_val.type().id() == LogicalTypeId::VARCHAR);
	auto setting_str = setting_val.GetValue<string>();
	D_ASSERT(!setting_str.empty());
	return setting_str;
}

//! Outcome of an authentication/authorization callback: what it returned, or why it could not run
struct AuthCallbackResult {
	Value value;
	string error;

	bool Denied() const {
		return !error.empty() || value.IsNull() ||
		       (value.type().id() == LogicalTypeId::BOOLEAN && !value.GetValue<bool>());
	}
	unique_ptr<ErrorResponse> Failure(const string &check) const {
		if (error.empty()) {
			return make_uniq<ErrorResponse>(check + " failed");
		}
		return make_uniq<ErrorResponse>(check + " callback failed: " + error);
	}
};

template <typename... ARGS>
static AuthCallbackResult EvaluateAuthQuery(DatabaseInstance &db, const string &sql, ARGS... values) {
	AuthCallbackResult result;
	Connection dummy_connection(db);
	auto query_result = dummy_connection.Query(sql, values...);
	if (!query_result || query_result->HasError()) {
		result.error = query_result ? query_result->GetError() : "auth callback produced no result";
		return result;
	}
	auto chunk = query_result->Fetch();
	if (chunk && chunk->size() > 0) {
		result.value = chunk->GetValue(0, 0);
	}
	return result;
}

// Derive a stable, per-client reconnect identifier as HMAC-SHA256(server_hmac_key, client_id)
static string ComputeClientHash(const string &server_hmac_key, const string &client_id) {
	unsigned char digest[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES];
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(server_hmac_key.data(), server_hmac_key.size(), client_id.data(),
	                                        client_id.size(), reinterpret_cast<char *>(digest));
	return QuackHexEncode(digest, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}

string QuackServer::GenerateSessionId() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	return QuackRandomToken(*db);
}

static string ExtractQuery(QuackMessage &msg) {
	if (msg.Type() == MessageType::PREPARE_REQUEST) {
		return msg.Cast<PrepareRequestMessage>().Query();
	}
	return "";
}

bool ServerSupportsMessage(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
	case MessageType::PREPARE_REQUEST:
	case MessageType::FETCH_REQUEST:
	case MessageType::SEND_DATA_REQUEST:
	case MessageType::DISCONNECT_MESSAGE:
	case MessageType::CANCEL_REQUEST:
	case MessageType::ACKNOWLEDGEMENT:
	case MessageType::HEARTBEAT_REQUEST:
		return true;
	default:
		return false;
	}
}

bool MessageRequiresConnection(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
		return false;
	default:
		return true;
	}
}

unique_ptr<QuackMessage> QuackServer::HandleMessage(MemoryStream &read_stream) {
	unique_ptr<QuackMessage> response;
	try {
		response = DispatchMessage(read_stream);
	} catch (std::exception &ex) {
		// Letting this escape to httplib would make it an HTTP 500, which the client can only report as a
		// transport failure ("Failed to send message"). Exceptions from HANDLING a message are caught below,
		// where they can still be logged; this catches the ones from decoding it.
		response = make_uniq<ErrorResponse>(ErrorData(ex));
	} catch (...) {
		response = make_uniq<ErrorResponse>("Unknown error while handling request");
	}
	if (response->Type() == MessageType::ERROR_RESPONSE) {
		// tell the client whether we are still usable - ask the database rather than guess from the exception
		// type, it is the one that decides it has been invalidated
		auto db = db_ptr.lock();
		response->Cast<ErrorResponse>().SetMustInvalidate(!db || ValidChecker::IsInvalidated(*db));
	}
	return response;
}

// main switcheroo happens here
unique_ptr<QuackMessage> QuackServer::DispatchMessage(MemoryStream &read_stream) {
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}
	if (ValidChecker::IsInvalidated(*db)) {
		// bail out before touching the database: the authentication query cannot run either, so a client
		// reconnecting to an invalidated server would otherwise be told its token was rejected
		return make_uniq<ErrorResponse>("The server database has been invalidated and the server must be "
		                                "restarted before it can be used again. Original error: \"%s\"",
		                                ValidChecker::Get(*db).InvalidatedMessage());
	}
	auto &logger = Logger::Get(*db);
	bool should_log = logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL);

	int64_t start_time = 0;
	if (should_log) {
		start_time = QuackNowMillis();
	}

	// start deserializing the message
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// first read the header
	auto header = QuackMessage::DeserializeHeader(deserializer);

	// validate if the server can handle this type of message - the server cannot handle all message types
	if (!ServerSupportsMessage(header.type)) {
		return make_uniq<ErrorResponse>("Unsupported message type for server");
	}

	// if the message requires it, obtain a connection
	// these are basically all messages aside from connect request
	shared_ptr<QuackConnection> connection;
	if (MessageRequiresConnection(header.type)) {
		connection = GetConnection(header.connection_id);
		if (!connection) {
			return make_uniq<ErrorResponse>("Invalid connection id");
		}
	}

	SweepExpiredCaches(*db);

	// now deserialize the actual message, and any raw chunk blob that follows it
	auto received_message = QuackMessage::DeserializeMessage(deserializer, header);
	if (connection) {
		// Only supported, structurally valid messages for an existing session renew its lease.
		if (!RenewConnectionLease(header.connection_id, connection)) {
			return make_uniq<ErrorResponse>("Connection heartbeat lease expired");
		}
	}

	// an exception raised while handling the message becomes the response, so it reaches the client with the
	// type it was raised with (and still shows up in the log below)
	unique_ptr<QuackMessage> response;
	try {
		response = HandleMessageInternal(*db, *received_message, connection);
	} catch (std::exception &ex) {
		response = make_uniq<ErrorResponse>(ErrorData(ex));
	}

	if (should_log) {
		auto duration_ms = QuackNowMillis() - start_time;
		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		auto client_id_hash = connection ? connection->client_id_hash : string();
		auto msg = QuackLogType::ConstructLogMessage(header.type, header.connection_id, client_id_hash,
		                                             header.client_query_id, ExtractQuery(*received_message), "",
		                                             duration_ms, response->Type(), error);
		logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
	}

	return response;
}

unique_ptr<QuackMessage> QuackServer::HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
                                                            optional_ptr<QuackConnection> connection_p) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		// The server speaks exactly QUACK_VERSION; reject unless the client's [min, max] range includes it.
		if (connection_request_message.MinimumSupportedQuackVersion() > QUACK_VERSION ||
		    connection_request_message.MaximumSupportedQuackVersion() < QUACK_VERSION) {
			return make_uniq<ErrorResponse>(StringUtil::Format(
			    "Unsupported Quack version - server only supports version %llu of quack", QUACK_VERSION));
		}
		auto heartbeat_timeout_seconds = connection_request_message.HeartbeatTimeoutSeconds();
		if (heartbeat_timeout_seconds == 0 || heartbeat_timeout_seconds > MAX_HEARTBEAT_TIMEOUT_SECONDS) {
			return make_uniq<ErrorResponse>(StringUtil::Format(
			    "heartbeat_timeout out of range - must be between 1 and %llu seconds", MAX_HEARTBEAT_TIMEOUT_SECONDS));
		}
		string session_id = GenerateSessionId();
		auto auth = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?, ?)", GetSettingString(db, "quack_authentication_function")),
		    Value(session_id), Value(connection_request_message.AuthString()), Value(Token()));
		if (auth.Denied()) {
			return auth.Failure("Authentication");
		}
		string client_id_hash;
		if (!connection_request_message.ClientId().empty()) {
			client_id_hash = ComputeClientHash(server_hmac_key, connection_request_message.ClientId());
		}
		return make_uniq<ConnectionResponseMessage>(
		    CreateNewConnection(session_id, client_id_hash, heartbeat_timeout_seconds), heartbeat_timeout_seconds);
	}
	case MessageType::DISCONNECT_MESSAGE: {
		auto &connection = *connection_p;
		if (!DisconnectConnection(connection.session_id)) {
			return make_uniq<ErrorResponse>("Connection does not exist / already disconnected");
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		auto &connection = *connection_p;

		// TODO do not do this if there is no fun set
		auto authz = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
		    Value(prepare_request_message.ConnectionId()), Value(prepare_request_message.Query()));
		if (authz.Denied()) {
			return authz.Failure("Authorization");
		}
		auto effective_sql = (authz.value.type().id() == LogicalTypeId::VARCHAR) ? authz.value.GetValue<string>()
		                                                                         : prepare_request_message.Query();

		auto query_uuid = prepare_request_message.QueryUUID();
		// Internal catalog traffic carries uuid 0. It must never displace the client's retained result.
		bool client_query = query_uuid != hugeint_t {0, 0};

		hugeint_t cached_uuid {0, 0};
		{
			std::unique_lock<std::mutex> lock(connection.lock);
			if (connection.result_cache) {
				cached_uuid = connection.result_cache->query_uuid;
			}
		}
		// The DuckDB connection runs one statement at a time, so a result still producing has to finish
		// before this statement can start. Its buffer cap goes: it finishes into memory instead of
		// parking on a client that is busy with this PREPARE. The statement under the same uuid (internal
		// traffic reuses 0) is superseded outright. Results the client is done with are released two
		// PREPAREs after it saw their end, so a parallel scan's racing final FETCH still gets its answer;
		// the replay cache pins its own result for as long as it lives.
		DetachedResultStream superseded;
		vector<DetachedResultStream> released;
		{
			lock_guard<mutex> guard(connection.statements_lock);
			connection.prepare_count++;
			superseded = DetachStatementLocked(connection, query_uuid, "superseded by a new query", true);
			for (auto it = connection.statements.begin(); it != connection.statements.end();) {
				auto &state = *it->second;
				bool exhausted = state.exhausted_at.IsValid() &&
				                 connection.prepare_count >= state.exhausted_at.GetIndex() + 2;
				if (exhausted && it->first != cached_uuid) {
					released.push_back(DetachedResultStream {std::move(state.stream), std::move(state.thread)});
					it = connection.statements.erase(it);
					continue;
				}
				if (state.stream) {
					state.stream->buffer.LiftCapacity();
				}
				++it;
			}
		}
		JoinDetachedStatement(superseded);
		for (auto &entry : released) {
			JoinDetachedStatement(entry);
		}
		{
			std::unique_lock<std::mutex> lock(connection.lock);
			if (client_query) {
				// A new client query displaces the retained one, even if caching is now off. Its stream may
				// still serve a concurrent scan, so it only stops retaining rather than going away.
				DisplaceResultCache(connection);
			}
			connection.sql_query = prepare_request_message.Query();
			connection.query_state = QuackQueryState::ACTIVE;
			connection.query_started_at = Timestamp::GetCurrentTimestamp();
			connection.query_uuid = query_uuid;
		}

		if (prepare_request_message.PrepareOnly()) {
			// Schema-only: bind the query to resolve its result schema without executing it. No statement
			// is installed, so there is nothing to fetch, release or abort later. The bind takes the
			// statement lock like a statement would: the DuckDB connection is shared with whatever runs
			// now, and that statement has just been told to finish.
			unique_ptr<PreparedStatement> prepared;
			{
				lock_guard<mutex> guard(connection.statement_lock);
				prepared = connection.duckdb_connection->Prepare(effective_sql);
			}
			auto fail = [&](ErrorData error) {
				std::unique_lock<std::mutex> lock(connection.lock);
				connection.query_state = QuackQueryState::CANCELLED;
				connection.sql_query = "";
				return make_uniq<ErrorResponse>(std::move(error));
			};
			if (prepared->HasError()) {
				return fail(prepared->GetErrorObject());
			}
			vector<string> names;
			for (auto &name : prepared->GetNames()) {
				names.push_back(name.GetIdentifierName());
			}
			if (names.empty()) {
				return fail(ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
			}
			auto types = prepared->GetTypes();
			connection.query_state = QuackQueryState::FINISHED;
			return make_uniq<PrepareResponseMessage>(types, names, vector<unique_ptr<DataChunkWrapper>>(),
			                                         /*needs_more_fetch=*/false, hugeint_t(0));
		}

		auto stream = make_shared_ptr<QuackResultStream>();
		// One buffer exists for each connection, so a fixed size does not suit every server. Core
		// also sizes an optional buffer at a quarter of the memory limit. 0 still means no limit.
		auto producer_bytes =
		    QuackGetUBigintSetting(db, "quack_fetch_producer_buffer_bytes", QUACK_FETCH_PRODUCER_BUFFER_BYTES_DEFAULT);
		auto memory_cap = BufferManager::GetBufferManager(db).GetOperatorMemoryLimit() / 4;
		if (producer_bytes > 0 && memory_cap > 0) {
			producer_bytes = MinValue<idx_t>(producer_bytes, memory_cap);
		}
		stream->buffer.SetCapacity(producer_bytes);
		bool retain_result = client_query && ServerCachingEnabled(db);
		if (retain_result) {
			// hold every served payload, instead of releasing it once the client acks
			stream->RetainAll();
		}
		{
			// Install under one lock. A concurrent PREPARE under this uuid can have installed a producer
			// since the detach above; it loses, and an assignment over a joinable std::thread would stop
			// the process.
			lock_guard<mutex> guard(connection.statements_lock);
			superseded = DetachStatementLocked(connection, query_uuid, "superseded by a new query", true);
			auto state = make_uniq<QuackStatementState>();
			state->stream = stream;
			state->uuid = query_uuid;
			state->thread = std::thread(DriveQuery, std::ref(connection), stream, effective_sql);
			connection.statements[query_uuid] = std::move(state);
		}
		JoinDetachedStatement(superseded);

		if (!stream->WaitBound()) {
			// planning failed before a collector existed
			auto error = stream->buffer.GetError();
			AbortStatement(connection, query_uuid, "query failed");
			std::unique_lock<std::mutex> lock(connection.lock);
			connection.sql_query = "";
			connection.query_state = QuackQueryState::CANCELLED;
			return make_uniq<ErrorResponse>(error);
		}

		// Put the leading batches in the PREPARE response, so a small result needs one round trip
		// only. The FETCH indices then move down by the count this drain consumed.
		// A statement that waits for client data must answer before it has a row. The client can then
		// start to send. It asks for that with inline_rows = 0.
		idx_t max_inline_rows =
		    prepare_request_message.InlineRows().IsValid()
		        ? prepare_request_message.InlineRows().GetIndex()
		        : QuackGetUBigintSetting(db, "quack_prepare_inline_rows", QUACK_PREPARE_INLINE_ROWS_DEFAULT);
		vector<unique_ptr<DataChunkWrapper>> results;
		idx_t inline_rows = 0;
		idx_t consumed = 0;
		while (inline_rows < max_inline_rows) {
			QuackFetchPayload entry;
			auto status = stream->buffer.TryPopClaimed(consumed + 1, entry);
			if (status == QuackClaimPopStatus::BATCH) {
				consumed++;
				inline_rows += entry.rows;
				// Batches are stored as header-less chunk blobs. Only this drain decodes them.
				MemoryStream payload_stream(entry.payload->GetData(), entry.payload_size);
				payload_stream.SetPosition(QUACK_PAYLOAD_HEADER_BYTES);
				BinaryDeserializer payload_deserializer(payload_stream);
				for (auto &chunk : DecodeQuackChunkBlob(payload_deserializer, entry.chunk_count)) {
					results.push_back(make_uniq<DataChunkWrapper>(*chunk));
				}
				if (retain_result) {
					// These rows leave in the PREPARE response, so the header for a cache replay is written here.
					FetchResponseMessage header_message;
					header_message.SetChunkCount(entry.chunk_count);
					header_message.SetBatchIndex(consumed);
					auto body_start = QuackPrependHeader(*entry.payload, header_message);
					stream->Retain(consumed, std::move(entry.payload), body_start, entry.rows);
				}
				continue;
			}
			if (status == QuackClaimPopStatus::FINISHED) {
				break;
			}
			if (status == QuackClaimPopStatus::ERRORED) {
				auto error = stream->buffer.GetError();
				AbortStatement(connection, query_uuid, "query failed");
				std::unique_lock<std::mutex> lock(connection.lock);
				connection.sql_query = "";
				connection.query_state = QuackQueryState::CANCELLED;
				return make_uniq<ErrorResponse>(error);
			}
			stream->buffer.WaitForBatch(consumed + 1);
		}
		stream->prepare_batches = consumed;
		if (retain_result) {
			// Created for every cached client query, also one with no rows. An empty result then
			// reports 0, and not "nothing cached".
			{
				std::unique_lock<std::mutex> lock(connection.lock);
				connection.result_cache =
				    make_uniq<QuackResultCache>(prepare_request_message.Query(), prepare_request_message.QueryUUID(),
				                                stream, connection.live_caches);
				RegisterCacheForExpiry(connection);
			}
			SyncResultCache(connection, *stream, db);
		}
		// Report more unless the drain saw a CLEAN end. A wrong true costs one more FETCH. A wrong
		// false on an errored buffer would report a truncated result as complete.
		auto needs_more_fetch = !stream->buffer.Exhausted() || stream->buffer.HasError();
		if (!needs_more_fetch) {
			// served whole inside this response: the client never FETCHes it
			connection.query_state = QuackQueryState::FINISHED;
			MarkExhausted(connection, query_uuid);
		}
		return make_uniq<PrepareResponseMessage>(stream->types, stream->names, std::move(results), needs_more_fetch,
		                                         query_uuid);
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		auto &connection = *connection_p;

		// FETCH touches only the buffer and the served map, so it needs neither lock on the way in.
		shared_ptr<QuackResultStream> stream;
		ErrorData abort_error;
		{
			lock_guard<mutex> guard(connection.statements_lock);
			auto entry = connection.statements.find(fetch_request_message.uuid);
			if (entry != connection.statements.end()) {
				stream = entry->second->stream;
				abort_error = entry->second->abort_error;
			}
		}
		if (!stream && abort_error.HasError()) {
			return make_uniq<ErrorResponse>(abort_error);
		}
		if (!stream) {
			return make_uniq<ErrorResponse>("Result has been closed");
		}
		if (fetch_request_message.batch_index == 0) {
			return make_uniq<ErrorResponse>("FETCH_REQUEST is missing its batch index");
		}

		// The request names its batch, so add back the batches PREPARE consumed inline. Served
		// payloads stay until the client acks them, so a transport retry gets the SAME batch.
		auto dense_index = fetch_request_message.batch_index + stream->prepare_batches;
		auto dense_ack = fetch_request_message.ack_index + stream->prepare_batches;
		{
			lock_guard<mutex> guard(stream->serve_lock);
			// The client has everything up to the ack and cannot ask for it again. A result cache
			// keeps the payloads for longer.
			if (!stream->retain_all) {
				auto last = stream->served.upper_bound(dense_ack);
				for (auto it = stream->served.begin(); it != last; ++it) {
					stream->retained_rows -= it->second.rows;
				}
				stream->served.erase(stream->served.begin(), last);
			}
		}

		while (true) {
			QuackClaimPopStatus status = QuackClaimPopStatus::EMPTY;
			QuackFetchPayload entry;
			shared_ptr<MemoryStream> served_batch;
			idx_t served_start = 0;
			{
				// One critical section for the check, the pop and the retain. A concurrent retry of the
				// same index cannot reach FINISHED while the first serve runs.
				lock_guard<mutex> guard(stream->serve_lock);
				auto retained = stream->served.find(dense_index);
				if (retained != stream->served.end()) {
					// the header with the client-visible index is already written
					served_batch = retained->second.payload;
					served_start = retained->second.body_start;
				} else {
					status = stream->buffer.TryPopClaimed(dense_index, entry);
					if (status == QuackClaimPopStatus::BATCH) {
						// write the header with the client-visible index, then keep the payload for a retry
						FetchResponseMessage header_message;
						header_message.SetChunkCount(entry.chunk_count);
						header_message.SetBatchIndex(fetch_request_message.batch_index);
						served_start = QuackPrependHeader(*entry.payload, header_message);
						shared_ptr<MemoryStream> payload(std::move(entry.payload));
						stream->served.emplace(dense_index,
						                       QuackResultStream::RetainedPayload {payload, served_start, entry.rows});
						stream->retained_rows += entry.rows;
						served_batch = std::move(payload);
					}
				}
			}
			if (served_batch) {
				// outside serve_lock, because the connection's state lock guards the cache
				SyncResultCache(connection, *stream, db);
				return make_uniq<QuackRawPayloadResponse>(MessageType::FETCH_RESPONSE, std::move(served_batch),
				                                          served_start);
			}
			if (status == QuackClaimPopStatus::ERRORED) {
				return make_uniq<ErrorResponse>(stream->buffer.GetError());
			}
			if (status == QuackClaimPopStatus::FINISHED) {
				// The stream ended below this index. The terminal response carries the total, so the
				// client can check that it got every batch.
				MarkExhausted(connection, fetch_request_message.uuid);
				{
					std::unique_lock<std::mutex> lock(connection.lock);
					if (connection.query_uuid == fetch_request_message.uuid &&
					    connection.query_state == QuackQueryState::ACTIVE) {
						connection.query_state = QuackQueryState::FINISHED;
					}
				}
				auto response = make_uniq<FetchResponseMessage>();
				if (stream->announced_total.IsValid()) {
					response->SetTotalBatches(stream->announced_total.GetIndex() - stream->prepare_batches);
				}
				return std::move(response);
			}
			stream->buffer.WaitForBatch(dense_index);
		}
	}

	case MessageType::SEND_DATA_REQUEST: {
		auto &send_data_message = received_message.Cast<SendDataRequestMessage>();
		auto &connection = *connection_p;

		// The streams live on this connection's session, so a batch is authorized by its connection id.
		auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context);
		auto stream = session_state ? session_state->Streams().Find(send_data_message.StreamId()) : nullptr;
		if (!stream) {
			return make_uniq<ErrorResponse>("No active data stream '%s'", send_data_message.StreamId());
		}
		{
			// the statement can fail before its scan sees the stream error
			shared_ptr<QuackResultStream> running;
			{
				lock_guard<mutex> guard(connection.statements_lock);
				running = connection.running_stream;
			}
			if (running && running->buffer.HasError()) {
				return make_uniq<ErrorResponse>(running->buffer.GetError());
			}
		}

		auto &incoming_chunks = send_data_message.Chunks();
		if (!incoming_chunks.empty()) {
			if (!send_data_message.BatchIndex().IsValid()) {
				return make_uniq<ErrorResponse>("send_data_request is missing its batch index");
			}
			// The claim buffer drops a duplicate index, so a retry of the same batch is safe.
			stream->buffer.PushBatch(send_data_message.BatchIndex().GetIndex(), std::move(incoming_chunks));
		}

		if (!send_data_message.TotalBatches().IsValid()) {
			if (stream->buffer.HasError()) {
				return make_uniq<ErrorResponse>(stream->buffer.GetError());
			}
			return make_uniq<SendDataResponseMessage>(); // accept_budget unset = unbounded (future flow control)
		}

		// The terminal message. Closing against a count makes a short stream fail loudly; a repeat
		// closes against the same count and gets the same answer. The statement's result, and any
		// later failure, come back through FETCH.
		stream->buffer.Finish(send_data_message.TotalBatches());
		if (stream->buffer.HasError()) {
			return make_uniq<ErrorResponse>(stream->buffer.GetError());
		}
		return make_uniq<SendDataResponseMessage>();
	}
	case MessageType::CANCEL_REQUEST: {
		auto &cancel_request_message = received_message.Cast<CancelRequestMessage>();
		auto &connection = *connection_p;
		auto uuid = cancel_request_message.query_uuid;
		if (uuid == hugeint_t {0, 0}) {
			// {0,0} is a wildcard — cancel whatever runs on this connection, and every result it holds:
			// Interrupt() acts on the whole DuckDB connection, so it cannot spare the others.
			AbortAllStatements(connection, "Interrupted: query was cancelled");
			connection.duckdb_connection->Interrupt();
			std::unique_lock<std::mutex> lock(connection.lock);
			connection.query_state = QuackQueryState::CANCELLED;
			connection.ClearResultCache();
			return make_uniq<SuccessResponse>();
		}
		bool pending;
		{
			lock_guard<mutex> guard(connection.statements_lock);
			pending = connection.statements.find(uuid) != connection.statements.end();
		}
		hugeint_t current_uuid;
		{
			std::unique_lock<std::mutex> lock(connection.lock);
			current_uuid = connection.query_uuid;
		}
		if (!pending && current_uuid != uuid) {
			return make_uniq<ErrorResponse>("Attempted to cancel a different query with id '%s' instead of '%s'", uuid,
			                                current_uuid);
		}
		// Interrupt() cannot wake a producer parked on the buffer's capacity. The abort's SetError
		// releases it, and interrupts the query only if this is the statement that runs. A client finds
		// a cancel by the "Interrupt" text.
		AbortStatement(connection, uuid, "Interrupted: query was cancelled");
		std::unique_lock<std::mutex> lock(connection.lock);
		if (connection.query_uuid == uuid) {
			connection.query_state = QuackQueryState::CANCELLED;
		}
		if (connection.result_cache && connection.result_cache->query_uuid == uuid) {
			connection.ClearResultCache();
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::ACKNOWLEDGEMENT: {
		auto &acknowledgement_message = received_message.Cast<AcknowledgementMessage>();
		auto &connection = *connection_p;
		auto uuid = acknowledgement_message.QueryUUID();
		{
			std::unique_lock<std::mutex> lock(connection.lock);
			auto &cache = connection.result_cache;
			if (cache && cache->query_uuid == uuid) {
				// The client confirmed it received the full result, the replay cache has served its purpose
				if (cache->stream) {
					cache->stream->DropRetention();
				}
				connection.ClearResultCache();
			}
		}
		// The client is done with this result, whether it read it whole or abandoned the scan: release
		// it. A producer still parked on it would otherwise hold the connection until the next statement,
		// which would then have to wait for the whole abandoned result. Acknowledging a result that is
		// already gone is a no-op: the client only ever means "I am done with this".
		AbortStatement(connection, uuid, "Result was acknowledged", true);
		return make_uniq<SuccessResponse>();
	}
	case MessageType::HEARTBEAT_REQUEST: {
		return make_uniq<SuccessResponse>();
	}
	default: {
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
} // namespace duckdb
