#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_storage.hpp"
#include "quack_data_stream.hpp"

#include "mbedtls_wrapper.hpp"

namespace duckdb {
QuackConnection::QuackConnection(string session_id_p) : session_id(std::move(session_id_p)) {
}

//! Finish + join + deregister a detached insert stream; returns any INSERT error. Call WITHOUT the lock.
ErrorData DetachedInsertStream::FinishAndJoin() {
	ErrorData error;
	if (!stream) {
		return error;
	}
	stream->Finish();
	if (thread.joinable()) {
		thread.join();
	}
	if (stream->HasError()) {
		error = stream->GetError();
	}
	QuackStreamRegistry::Get().Erase(id);
	return error;
}

//! Roll the INSERT back + join + deregister a detached insert stream. Call WITHOUT the lock.
void DetachedInsertStream::AbortAndJoin(const string &reason) {
	if (!stream) {
		return;
	}
	stream->SetError(ErrorData(ExceptionType::INVALID_INPUT, reason));
	if (thread.joinable()) {
		thread.join();
	}
	QuackStreamRegistry::Get().Erase(id);
}

DetachedInsertStream QuackInsertState::Detach() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	DetachedInsertStream detached;
	detached.stream = std::move(stream);
	detached.thread = std::move(thread);
	detached.id = std::move(stream_id);
	stream.reset();
	stream_id.clear();
	return detached;
}

DetachedInsertStream QuackInsertState::DetachIfUnrelated(const string &msg_stream_id) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	DetachedInsertStream detached;
	if (!stream || stream_id == msg_stream_id) {
		return detached; // nothing active, or this message continues the active stream
	}
	detached.stream = std::move(stream);
	detached.thread = std::move(thread);
	detached.id = std::move(stream_id);
	stream.reset();
	stream_id.clear();
	return detached;
}

ErrorData QuackInsertState::Finalize(optional_idx watermark) {
	auto detached = Detach();
	if (watermark.IsValid() && detached.stream) {
		detached.stream->SetWatermarkAndDrain(watermark);
	}
	return detached.FinishAndJoin();
}

shared_ptr<QuackDataStream> QuackInsertState::StreamForDeadRangeOrBuffer(const string &sid, idx_t lo, idx_t hi) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (stream && stream_id == sid) {
		return stream;
	}
	// Marker arrived before its stream existed (reordering): buffer it for when the stream is created.
	if (pending_marker_stream_id != sid) {
		pending_marker_stream_id = sid;
		pending_dead_ranges.clear();
	}
	pending_dead_ranges.emplace_back(lo, hi);
	return nullptr;
}

QuackConnection::~QuackConnection() {
	// Abort + join any in-flight INSERT before members are destroyed.
	insert.Detach().AbortAndJoin("connection closed during insert");
	pending_results.clear();
}

//! Background thread: runs the INSERT that drains `stream` via scan_data_from_quack_client, holding
//! the connection lock for the statement's duration (one transactional statement -> atomic).
static void MaterializeLiveResults(DatabaseInstance &db, QuackConnection &connection);

static void RunInsertStatement(QuackConnection &connection, shared_ptr<QuackDataStream> stream, string stream_id,
                               string schema_name, string table_name) {
	try {
		unique_lock<mutex> lock(connection.lock);
		// This INSERT takes over the DuckDB connection, which supports one open stream: drain any
		// pending streaming result to a buffer first so later FETCHes still see their rows.
		MaterializeLiveResults(*connection.duckdb_connection->context->db, connection);
		auto sql = StringUtil::Format("INSERT INTO %s.%s SELECT * FROM scan_data_from_quack_client(%s)",
		                              SQLIdentifier(schema_name), SQLIdentifier(table_name), SQLString(stream_id));
		auto result = connection.duckdb_connection->Query(sql);
		if (result->HasError()) {
			stream->SetError(result->GetErrorObject());
		}
	} catch (std::exception &ex) {
		stream->SetError(ErrorData(ex));
	}
	// Make sure the consumer side is released even on an unexpected early return.
	stream->Finish();
}

//! Stream id a SEND_DATA/FINALIZE belongs to, or "" for any other message.
static string StreamIdForMessage(QuackMessage &msg) {
	if (msg.Type() == MessageType::SEND_DATA_REQUEST) {
		auto &m = msg.Cast<SendDataRequestMessage>();
		return QuackStreamRegistry::MakeId(m.ConnectionId(), m.QueryUUID());
	}
	if (msg.Type() == MessageType::FINALIZE) {
		auto &m = msg.Cast<FinalizeMessage>();
		return QuackStreamRegistry::MakeId(m.ConnectionId(), m.QueryUUID());
	}
	return string();
}

void QuackServer::ValidateToken(const string &token) {
	if (token.size() < 4) {
		throw InvalidInputException("Quack server token must be at least 4 characters long");
	}
}

QuackServer::QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : db_ptr(context_p.db), uri(uri_p), token(token_p) {
	ValidateToken(token);
	server_hmac_key = GenerateRandomToken(*context_p.db);
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

vector<QuackConnectionSnapshot> QuackServer::GetActiveConnectionSnap() {
	vector<QuackConnectionSnapshot> result;
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	for (auto &[id, conn] : active_connections) {
		QuackConnectionSnapshot snapshot;
		snapshot.session_id = conn->session_id;
		snapshot.client_id_hash = conn->client_id_hash;
		snapshot.sql_query = conn->sql_query;
		snapshot.query_state = conn->query_state;
		snapshot.query_started_at = conn->query_started_at;
		result.push_back(std::move(snapshot));
	}
	return result;
}

shared_ptr<QuackConnection> QuackServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second;
	}
	return nullptr;
}

string QuackServer::CreateNewConnection(const string &session_id, const string &client_id_hash) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto new_connection = make_shared_ptr<QuackConnection>(session_id);
	new_connection->client_id_hash = client_id_hash;
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	new_connection->duckdb_connection->context->config.enable_progress_bar = false;
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

	auto lookup_result = config.TryGetCurrentSetting(setting_name, setting_val);
	D_ASSERT(lookup_result);
	D_ASSERT(setting_val.type().id() == LogicalTypeId::VARCHAR);
	auto setting_str = setting_val.GetValue<string>();
	D_ASSERT(!setting_str.empty());
	return setting_str;
}

template <typename... ARGS>
static Value EvaluateAuthQuery(DatabaseInstance &db, const string &sql, ARGS... values) {
	Connection dummy_connection(db);
	auto auth_result = dummy_connection.Query(sql, values...);
	if (!auth_result || auth_result->HasError()) {
		return Value(false);
	}
	auto auth_result_chunk = auth_result->Fetch();
	if (!auth_result_chunk || auth_result_chunk->size() == 0) {
		return Value(false);
	}
	return auth_result_chunk->GetValue(0, 0);
}

static constexpr idx_t kTokenBytes = 16; // 128 bits

static string HexEncode(const data_t *bytes, idx_t n) {
	string result(n * 2, '\0');
	for (idx_t i = 0; i < n; i++) {
		result[2 * i] = Blob::HEX_TABLE[bytes[i] >> 4];
		result[2 * i + 1] = Blob::HEX_TABLE[bytes[i] & 0x0F];
	}
	return result;
}

// Derive a stable, per-client reconnect identifier as HMAC-SHA256(server_hmac_key, client_id)
static string ComputeClientHash(const string &server_hmac_key, const string &client_id) {
	unsigned char digest[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES];
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(server_hmac_key.data(), server_hmac_key.size(), client_id.data(),
	                                        client_id.size(), reinterpret_cast<char *>(digest));
	return HexEncode(digest, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}

string QuackServer::GenerateRandomToken(DatabaseInstance &db) {
	auto encryption_util = db.GetEncryptionUtil(false);
	auto metadata =
	    make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes, EncryptionTypes::EncryptionVersion::NONE);
	auto rng = encryption_util->CreateEncryptionState(std::move(metadata));

	data_t bytes[kTokenBytes];
	rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
}

string QuackServer::GenerateSessionId() {
	{
		std::lock_guard<std::mutex> lock(session_id_rng_mutex);
		if (!session_id_rng) {
			auto db = db_ptr.lock();
			if (!db) {
				throw InternalException("Database was closed");
			}
			auto encryption_util = db->GetEncryptionUtil(false);
			auto metadata = make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes,
			                                                   EncryptionTypes::EncryptionVersion::NONE);
			session_id_rng = encryption_util->CreateEncryptionState(std::move(metadata));
		}
	}

	data_t bytes[kTokenBytes];
	session_id_rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
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
	case MessageType::CLOSE_RESULT_REQUEST:
	case MessageType::FINALIZE:
	case MessageType::ACKNOWLEDGEMENT:
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

// main switcheroo happens here
unique_ptr<QuackMessage> QuackServer::HandleMessage(MemoryStream &read_stream) {
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
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

	// now deserialize the actual message
	auto received_message = QuackMessage::DeserializeMessage(deserializer, header);

	// process the message
	auto response = HandleMessageInternal(*db, *received_message, connection);

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

// Accumulate whole chunks until `max_rows` is reached (row-based like the send path, so sparse
// filtered chunks don't shrink the batch). Resets query_result once the cursor is exhausted.
static vector<unique_ptr<DataChunkWrapper>> CreateBatch(Allocator &allocator, unique_ptr<QueryResult> &query_result,
                                                        idx_t max_rows) {
	vector<unique_ptr<DataChunkWrapper>> results;

	idx_t rows = 0;
	while (rows < max_rows) {
		auto result_chunk = query_result->Fetch();
		// error case
		if (!result_chunk && query_result->HasError()) {
			results.clear();
			return results;
		}
		// we are done case
		if (!result_chunk || result_chunk->size() == 0) {
			query_result.reset();
			break;
		}
		rows += result_chunk->size();
		results.push_back(make_uniq<DataChunkWrapper>(*result_chunk));
	}
	return results;
}

//! Move every live streaming result out of the way of a new query on this connection: the
//! underlying DuckDB connection supports one open stream, and executing another query would
//! invalidate it. Remaining batches are drained into a buffer-managed collection (spilling to
//! disk under memory pressure) and served from there by subsequent FETCHes.
static void MaterializeLiveResults(DatabaseInstance &db, QuackConnection &connection) {
	for (auto &entry : connection.pending_results) {
		auto &pending = entry.second;
		if (!pending.live) {
			continue;
		}
		auto collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(db), pending.live->types);
		while (true) {
			auto chunk = pending.live->Fetch();
			if (!chunk && pending.live->HasError()) {
				// surface the error on the next FETCH of this result
				pending.error = pending.live->GetErrorObject();
				collection.reset();
				break;
			}
			if (!chunk || chunk->size() == 0) {
				break;
			}
			collection->Append(*chunk);
		}
		pending.live.reset();
		if (collection) {
			pending.buffered = std::move(collection);
			pending.buffered->InitializeScan(pending.buffered_scan, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		}
	}
}

//! Drop fully-served results. A result is only dropped two PREPAREs after it was exhausted: a
//! parallel client scan may still have an in-flight FETCH racing with the empty batch that ended
//! it, and that FETCH must see an empty response rather than "Result has been closed".
static void CleanupExhaustedResults(QuackConnection &connection) {
	for (auto it = connection.pending_results.begin(); it != connection.pending_results.end();) {
		auto &exhausted_at = it->second.exhausted_at;
		if (exhausted_at.IsValid() && connection.prepare_count >= exhausted_at.GetIndex() + 2) {
			it = connection.pending_results.erase(it);
		} else {
			++it;
		}
	}
}

//! Pull up to `max_rows` worth of chunks out of an already-drained result.
static vector<unique_ptr<DataChunkWrapper>> CreateBufferedBatch(QuackPendingResult &pending, idx_t max_rows) {
	vector<unique_ptr<DataChunkWrapper>> results;
	idx_t rows = 0;
	DataChunk chunk;
	pending.buffered->InitializeScanChunk(pending.buffered_scan, chunk);
	while (rows < max_rows) {
		chunk.Reset();
		pending.buffered->Scan(pending.buffered_scan, chunk);
		if (chunk.size() == 0) {
			pending.buffered.reset();
			break;
		}
		rows += chunk.size();
		results.push_back(make_uniq<DataChunkWrapper>(chunk));
	}
	return results;
}

unique_ptr<QuackMessage> QuackServer::HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
                                                            optional_ptr<QuackConnection> connection_p) {
	if (connection_p) {
		// A message unrelated to the active insert stream means it was abandoned (client source failed, no
		// FINALIZE) — abort it so it rolls back and releases the connection lock.
		connection_p->insert.DetachIfUnrelated(StreamIdForMessage(received_message))
		    .AbortAndJoin("insert stream abandoned");
	}
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		// The server speaks exactly QUACK_VERSION; reject unless the client's [min, max] range includes it.
		if (connection_request_message.MinimumSupportedQuackVersion() > QUACK_VERSION ||
		    connection_request_message.MaximumSupportedQuackVersion() < QUACK_VERSION) {
			return make_uniq<ErrorResponse>(StringUtil::Format(
			    "Unsupported Quack version - server only supports version %llu of quack", QUACK_VERSION));
		}
		string session_id = GenerateSessionId();
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?, ?)", GetSettingString(db, "quack_authentication_function")),
		    Value(session_id), Value(connection_request_message.AuthString()), Value(Token()));

		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authentication failed");
		}
		string client_id_hash;
		if (!connection_request_message.ClientId().empty()) {
			client_id_hash = ComputeClientHash(server_hmac_key, connection_request_message.ClientId());
		}
		return make_uniq<ConnectionResponseMessage>(CreateNewConnection(session_id, client_id_hash));
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
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
		    Value(prepare_request_message.ConnectionId()), Value(prepare_request_message.Query()));
		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authorization failed");
		}
		auto effective_sql = (auth_result.type().id() == LogicalTypeId::VARCHAR) ? auth_result.GetValue<string>()
		                                                                         : prepare_request_message.Query();

		std::unique_lock<std::mutex> lock(connection.lock);
		connection.prepare_count++;
		CleanupExhaustedResults(connection);
		// Other results pending on this connection keep working: their remaining batches are
		// drained into buffered collections before this query takes over the live stream.
		MaterializeLiveResults(db, connection);
		connection.sql_query = prepare_request_message.Query();
		connection.query_state = QuackQueryState::ACTIVE;
		connection.query_started_at = Timestamp::GetCurrentTimestamp();

		if (prepare_request_message.PrepareOnly()) {
			// Schema-only: bind the query to resolve its result schema without executing it. No
			// pending result is created, so there is nothing to fetch, drop or drain later.
			auto prepared = connection.duckdb_connection->Prepare(effective_sql);
			if (prepared->HasError()) {
				connection.query_state = QuackQueryState::CANCELLED;
				connection.sql_query = "";
				return make_uniq<ErrorResponse>(prepared->GetErrorObject());
			}
			vector<string> names;
			for (auto &name : prepared->GetNames()) {
				names.push_back(name.GetIdentifierName());
			}
			if (names.empty()) {
				connection.query_state = QuackQueryState::CANCELLED;
				connection.sql_query = "";
				return make_uniq<ErrorResponse>("Query did not return any columns");
			}
			auto types = prepared->GetTypes();
			// The planner already sized this query; ship its estimate so the client's optimizer
			// does not fall back to a 1-row default for the remote relation.
			idx_t estimated_cardinality = 0;
			if (prepared->data && prepared->data->physical_plan) {
				estimated_cardinality = prepared->data->physical_plan->Root().estimated_cardinality;
			}
			connection.query_state = QuackQueryState::FINISHED;
			return make_uniq<PrepareResponseMessage>(types, names, vector<unique_ptr<DataChunkWrapper>>(),
			                                         /*needs_more_fetch=*/false, hugeint_t(0), estimated_cardinality);
		}

		// The client mints a fresh UUID per PREPARE, so it keys this result uniquely.
		auto query_uuid = prepare_request_message.QueryUUID();
		connection.query_uuid = query_uuid;
		{
			auto query_result = connection.duckdb_connection->SendQuery(effective_sql);
			if (query_result->HasError()) {
				connection.sql_query = "";
				auto response = make_uniq<ErrorResponse>(query_result->GetErrorObject());
				connection.query_state = QuackQueryState::CANCELLED;
				//! Explicit move: converting unique_ptr<ErrorResponse> to unique_ptr<QuackMessage>
				//! on return is only an implicit move from gcc-13 (P1825); the deploy's jammy
				//! toolchain is gcc-12.
				return unique_ptr<QuackMessage>(std::move(response));
			}
			if (query_result->names.empty()) {
				connection.sql_query = "";
				connection.query_state = QuackQueryState::QUACK_ERROR;
				return make_uniq<ErrorResponse>("Query did not return any columns");
			}

			connection.pending_results[query_uuid].live = std::move(query_result);
		}
		auto &pending = connection.pending_results[query_uuid];

		Value max_rows_val;
		DBConfig::GetConfig(db).TryGetCurrentSetting("quack_fetch_batch_rows", max_rows_val);
		auto max_rows_per_batch = max_rows_val.GetValue<uint64_t>();

		auto names = pending.live->names;
		auto types = pending.live->types;

		auto results = CreateBatch(Allocator::Get(db), pending.live, max_rows_per_batch);
		if (pending.live && pending.live->HasError()) {
			D_ASSERT(results.empty());

			auto error_message = pending.live->GetErrorObject();
			connection.pending_results.erase(query_uuid);
			return make_uniq<ErrorResponse>(std::move(error_message));
		}
		// CreateBatch resets the cursor on exhaustion; a live cursor means more batches remain.
		auto needs_more_fetch = pending.live != nullptr;
		if (!needs_more_fetch) {
			// Fully served inside this response - the client will never FETCH it.
			connection.pending_results.erase(query_uuid);
			if (connection.query_state == QuackQueryState::ACTIVE) {
				connection.query_state = QuackQueryState::FINISHED;
			}
		}
		return make_uniq<PrepareResponseMessage>(types, names, std::move(results), needs_more_fetch, query_uuid);
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		auto &connection = *connection_p;
		std::unique_lock<std::mutex> lock(connection.lock);

		auto entry = connection.pending_results.find(fetch_request_message.uuid);
		if (entry == connection.pending_results.end()) {
			return make_uniq<ErrorResponse>("Result has been closed");
		}
		auto &pending = entry->second;
		if (connection.query_state == QuackQueryState::CANCELLED) {
			return make_uniq<ErrorResponse>("Query was interrupted");
		}
		if (pending.error.HasError()) {
			auto error_message = pending.error;
			connection.pending_results.erase(entry);
			return make_uniq<ErrorResponse>(std::move(error_message));
		}
		if (pending.live && pending.live->HasError()) {
			return make_uniq<ErrorResponse>(pending.live->GetErrorObject());
		}
		if (!pending.live && !pending.buffered) {
			// Exhausted, but kept briefly so a racing parallel FETCH sees an empty batch.
			return make_uniq<FetchResponseMessage>();
		}

		Value max_rows_val;
		DBConfig::GetConfig(db).TryGetCurrentSetting("quack_fetch_batch_rows", max_rows_val);
		auto max_rows_per_batch = max_rows_val.GetValue<uint64_t>();

		vector<unique_ptr<DataChunkWrapper>> results;
		if (pending.live) {
			results = CreateBatch(Allocator::Get(db), pending.live, max_rows_per_batch);
			if (pending.live && pending.live->HasError()) { // TODO this is duplicated
				D_ASSERT(results.empty());
				auto error_message = pending.live->GetErrorObject();
				connection.pending_results.erase(entry);
				return make_uniq<ErrorResponse>(std::move(error_message));
			}
		} else {
			results = CreateBufferedBatch(pending, max_rows_per_batch);
		}
		auto assigned_batch_index = pending.next_batch_index++;
		if (!pending.live && !pending.buffered) {
			pending.exhausted_at = connection.prepare_count;
			if (connection.query_state == QuackQueryState::ACTIVE) {
				connection.query_state = QuackQueryState::FINISHED;
			}
		}
		return make_uniq<FetchResponseMessage>(std::move(results), optional_idx(assigned_batch_index));
	}

	case MessageType::SEND_DATA_REQUEST: {
		auto &send_data_message = received_message.Cast<SendDataRequestMessage>();
		auto &connection = *connection_p;

		// we never execute this query, but throw it at the authorization function so it can check if this user gets to
		// insert into this table
		auto dummy_insert_query =
		    StringUtil::Format("INSERT INTO %s.%s VALUES (NULL)", SQLIdentifier(send_data_message.SchemaName()),
		                       SQLIdentifier(send_data_message.TableName()));

		// TODO do not do this if there is no fun set
		{
			auto auth_result = EvaluateAuthQuery(
			    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
			    Value(send_data_message.ConnectionId()), Value(dummy_insert_query));
			if (auth_result.IsNull() ||
			    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
				return make_uniq<ErrorResponse>("Authorization failed");
			}
		}

		// Lazily create the stream + background INSERT on the first message (stream is keyed by query_uuid).
		auto stream_id = QuackStreamRegistry::MakeId(send_data_message.ConnectionId(), send_data_message.QueryUUID());
		bool ordered = send_data_message.BatchIndex().IsValid();
		auto &incoming_chunks = send_data_message.Chunks();

		// Dead-range marker: no chunks, tells the server batches [lo, hi) are dead so the cursor can skip them.
		if (send_data_message.IsDeadRange()) {
			auto lo = send_data_message.BatchIndex().GetIndex();
			auto hi = send_data_message.DeadRangeEnd().GetIndex();
			auto dead_stream = connection.insert.StreamForDeadRangeOrBuffer(stream_id, lo, hi);
			if (dead_stream) {
				dead_stream->PushDeadRange(lo, hi);
			}
			return make_uniq<SendDataResponseMessage>();
		}

		shared_ptr<QuackDataStream> stream;
		vector<std::pair<idx_t, idx_t>> buffered_dead_ranges;
		{
			annotated_lock_guard<annotated_mutex> guard(connection.insert.lock);
			if (connection.insert.stream) {
				stream = connection.insert.stream;
			} else {
				if (incoming_chunks.empty()) {
					// Zero-chunk first message — the client shouldn't produce this (FlushBuffer skips empty
					// unstarted batches), but guard against it gracefully.
					return make_uniq<SendDataResponseMessage>();
				}
				auto types = incoming_chunks[0]->Chunk().GetTypes();
				stream = QuackStreamRegistry::Get().Create(stream_id, types, ordered);
				connection.insert.stream = stream;
				connection.insert.stream_id = stream_id;
				connection.insert.thread = std::thread(RunInsertStatement, std::ref(connection), stream, stream_id,
				                                       send_data_message.SchemaName(), send_data_message.TableName());
				// Apply any dead-range markers that arrived before the stream existed.
				if (connection.insert.pending_marker_stream_id == stream_id) {
					buffered_dead_ranges = std::move(connection.insert.pending_dead_ranges);
					connection.insert.pending_dead_ranges.clear();
					connection.insert.pending_marker_stream_id.clear();
				}
			}
		}

		for (auto &r : buffered_dead_ranges) {
			stream->PushDeadRange(r.first, r.second);
		}

		// Reference the chunks from the message. DataChunkWrapper.Deserialize uses DataChunk::Reference()
		// internally, so the underlying VectorBuffers are ref-counted and outlive the message.
		vector<unique_ptr<DataChunk>> owned_chunks;
		owned_chunks.reserve(incoming_chunks.size());
		for (auto &wrapper : incoming_chunks) {
			auto owned = make_uniq<DataChunk>();
			owned->InitializeEmpty(wrapper->Chunk().GetTypes());
			owned->Reference(wrapper->Chunk());
			owned_chunks.push_back(std::move(owned));
		}

		if (ordered) {
			stream->PushOrdered(std::move(owned_chunks), send_data_message.BatchIndex().GetIndex(),
			                    send_data_message.SequenceIndex(), send_data_message.IsLastInBatch(),
			                    send_data_message.BatchWatermark());
		} else {
			stream->PushUnordered(std::move(owned_chunks));
		}

		if (stream->HasError()) {
			auto error = connection.insert.Finalize();
			return make_uniq<ErrorResponse>(error);
		}
		return make_uniq<SendDataResponseMessage>(); // accept_budget unset = unbounded (future flow control)
	}
	case MessageType::FINALIZE: {
		auto &finalize_message = received_message.Cast<FinalizeMessage>();
		auto &connection = *connection_p;
		auto stream_id = QuackStreamRegistry::MakeId(finalize_message.ConnectionId(), finalize_message.QueryUUID());
		{
			annotated_lock_guard<annotated_mutex> guard(connection.insert.lock);
			if (connection.insert.stream_id != stream_id) {
				return make_uniq<SuccessResponse>(); // no matching stream (e.g. zero-chunk insert)
			}
		}
		auto error = connection.insert.Finalize(finalize_message.MinBatchWatermark());
		if (error.HasError()) {
			return make_uniq<ErrorResponse>(error);
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::CANCEL_REQUEST: {
		auto &cancel_request_message = received_message.Cast<CancelRequestMessage>();
		auto &connection = *connection_p;
		// {0,0} is a wildcard — cancel whatever query is running on this connection
		bool is_wildcard = cancel_request_message.query_uuid == hugeint_t {0, 0};
		if (!is_wildcard && connection.query_uuid != cancel_request_message.query_uuid &&
		    connection.pending_results.find(cancel_request_message.query_uuid) == connection.pending_results.end()) {
			return make_uniq<ErrorResponse>("Attempted to cancel a different query with id '%s' instead of '%s'",
			                                cancel_request_message.query_uuid, connection.query_uuid);
		}
		// Interrupt() acts on the whole DuckDB connection, so it cannot spare the other pending
		// results on it - drop them all rather than leave results that would fail mid-fetch.
		connection.duckdb_connection->Interrupt();
		connection.query_state = QuackQueryState::CANCELLED;
		connection.pending_results.clear();
		return make_uniq<SuccessResponse>();
	}
	case MessageType::CLOSE_RESULT_REQUEST: {
		auto &close_result_message = received_message.Cast<CloseResultRequestMessage>();
		auto &connection = *connection_p;
		std::unique_lock<std::mutex> lock(connection.lock);
		// Best-effort: a result already dropped (exhausted, or cancelled) is not an error, the
		// client only ever means "I am done with this".
		connection.pending_results.erase(close_result_message.query_uuid);
		return make_uniq<SuccessResponse>();
	}
	case MessageType::ACKNOWLEDGEMENT: {
		return make_uniq<SuccessResponse>();
	}
	default: {
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
} // namespace duckdb
