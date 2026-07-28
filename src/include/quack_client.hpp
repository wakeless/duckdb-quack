#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_periodic_worker.hpp"
#include "quack_uri.hpp"

#include "httplib.hpp"

#include <functional>

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

//! The active query's id, for log correlation. Invalid when no query runs (e.g. transaction start).
optional_idx QuackActiveClientQueryId(ClientContext &context);

class QuackClient {
public:
	explicit QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p);
	virtual ~QuackClient();

	//! Send a request and return the raw response, including error responses
	unique_ptr<QuackMessage> RawRequest(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		return RequestInternal(context, std::move(request_message));
	}

	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		auto response_message = RequestInternal(context, std::move(request_message));
		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				// if we get an error throw it immediately
				NoteError(response_message->Cast<ErrorResponse>());
				response_message->Cast<ErrorResponse>().Error().Throw();
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr_cast<QuackMessage, TARGET>(std::move(response_message));
	}

	//! Invalidate the owning connection if the error says the server's database died. Call before rethrowing.
	void NoteError(const ErrorResponse &error_response);

	//! The connection this client is checked out from - set by QuackClientWrapper. Null for the initial connect
	//! and for a one-shot quack_query.
	void SetOwnerConnection(optional_ptr<const QuackClientConnection> owner_connection_p) {
		owner_connection = owner_connection_p;
	}

	//! POST already-serialized request bytes and return the raw response body, throwing on transport failure.
	//! Lets an async sender serialize on a producer thread and perform the blocking POST from the ASYNC pool;
	//! pass context=nullptr when called off the execution thread (parameters fall back to the database).
	virtual string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) = 0;

	//! Best-effort teardown hint: bound the transport for subsequent (final DisconnectMessage) requests so a
	//! gone/half-open peer cannot block the destructor for the full, possibly hour-long, request timeout.
	//! Default no-op; the HTTPS client caps the timeout and skips retries. Not thread-safe — call on a client
	//! that is being torn down and will issue at most one more request.
	virtual void PrepareTeardownRequest() {
	}

	//! Encode a request (inject client_query_id when a query is active, then serialize) into `out`.
	//! Protocol-level and lock-free; the caller owns `out` and any locking around it.
	static void EncodeRequest(optional_ptr<ClientContext> context, QuackMessage &message, MemoryStream &out);

	//! Decode a response body (owned bytes) into a message. Protocol-level and lock-free.
	static unique_ptr<QuackMessage> DecodeResponse(const string &response_body);

	//! Emit a Quack request log entry through `logger`. Pass the query's context logger for per-query
	//! attribution (connection/transaction/query id columns); the db logger only fits context-less
	//! teardown. Safe from an async pool thread when handed a captured context logger.
	void LogRequest(Logger &logger, MessageType request_type, const string &connection_id, optional_idx client_query_id,
	                const string &query, int64_t duration_ms, MessageType response_type, const string &error);

	//! Stamp the logger for this client's HTTP transport-log entries. Set at checkout so a pooled client
	//! (incl. an async send whose pool thread has no ClientContext) logs under the checking-out query.
	void SetRequestLogger(shared_ptr<Logger> logger);

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token,
	                                                         string client_id, idx_t heartbeat_timeout_seconds);

	//! Resolve the effective client_id for a new connection
	static string ResolveClientId(ClientContext &context, optional_ptr<const Value> explicit_value);
	//! Resolve the requested heartbeat lease timeout for a new connection
	static idx_t ResolveHeartbeatTimeout(ClientContext &context, optional_ptr<const Value> explicit_value);

	//! Throw unless `client_id` is either empty ("no client_id") or >= 4 characters
	static void ValidateClientId(const string &client_id);
	//! Heartbeat leases must have a positive timeout.
	static void ValidateHeartbeatTimeout(idx_t heartbeat_timeout_seconds);

protected:
	//! Resolve the logger for a request: the context (per-query) logger when available, else the db logger.
	Logger &GetRequestLogger(optional_ptr<ClientContext> context);

	mutex request_mutex;
	MemoryStream write_stream;
	DatabaseInstance &db;
	QuackUri uri;
	//! HTTP transport-log logger, stamped at checkout (see SetRequestLogger).
	shared_ptr<Logger> request_logger;
	//! See SetOwnerConnection
	optional_ptr<const QuackClientConnection> owner_connection;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                                 unique_ptr<QuackMessage> request_message) = 0;
};

struct QuackClientWrapper {
	QuackClientWrapper(unique_ptr<QuackClient> client, shared_ptr<const QuackClientConnection> client_connection);
	~QuackClientWrapper();

	QuackClient &GetClient();

private:
	unique_ptr<QuackClient> client;
	shared_ptr<const QuackClientConnection> client_connection;
};

class QuackClientConnection : public enable_shared_from_this<QuackClientConnection> {
public:
	explicit QuackClientConnection(DatabaseInstance &db_p, unique_ptr<QuackClient> client_p, QuackUri uri_p,
	                               string connection_id_p, idx_t heartbeat_timeout_seconds_p, string token_p = {},
	                               string client_id_p = {}, idx_t max_connections_cached = 1);
	~QuackClientConnection();

	void CancelQuery(hugeint_t query_uuid);

	//! By value, under the lock: Reconnect() can replace the id while other threads are building
	//! requests, so handing out a reference into it lets a caller read the string mid-write.
	string ConnectionId() const {
		lock_guard<mutex> guard(lock);
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}
	idx_t HeartbeatTimeoutSeconds() const {
		return heartbeat_timeout_seconds;
	}

	//! Get a client (either a cached one, or open a new one if required).
	//! Throws once the server has told us its database is gone - see MarkServerInvalidated.
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Re-establish the server session after the server forgot it (e.g. a restart). Returns true
	//! when the connection now holds an id different from stale_connection_id - either because
	//! this call performed the handshake, or because another thread already did. Reconnects are
	//! serialized, so a herd of failing requests performs one handshake.
	bool Reconnect(ClientContext &context, const string &stale_connection_id) const;

	//! Whether an error response says the server no longer knows this session. Matches the raw
	//! message the server sent, not the decorated form Message() renders: the server raises it as a
	//! plain InvalidInputException with no extra info, so the text is the only thing that identifies it.
	static bool IsStaleSessionError(const ErrorResponse &error) {
		return error.Error().RawMessage() == "Invalid connection id";
	}

	//! Send a request built against the current connection id; when the server reports the
	//! session is gone, re-handshake once and resend. Only safe for requests that do not depend
	//! on server-side session state (a fresh PREPARE, a transaction-opening BEGIN); mid-result
	//! fetches, appends and commits must not, since that state is genuinely gone.
	template <class TARGET>
	unique_ptr<TARGET>
	RequestWithReconnect(ClientContext &context,
	                     const std::function<unique_ptr<QuackMessage>(const string &)> &message) const {
		for (idx_t attempt = 0;; attempt++) {
			auto current_id = ConnectionId();
			auto client_wrapper = GetClient(context);
			auto &client = client_wrapper->GetClient();
			auto response = client.RawRequest(context, message(current_id));
			if (response->Type() == MessageType::ERROR_RESPONSE) {
				auto &error = response->Cast<ErrorResponse>();
				// a server whose database died says so on every response; that is the opposite of a
				// forgotten session, and Reconnect() refuses it too
				client.NoteError(error);
				if (attempt == 0 && IsStaleSessionError(error) && Reconnect(context, current_id)) {
					continue;
				}
				error.Error().Throw();
			}
			if (response->Type() != TARGET::TYPE) {
				throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
				                  MessageTypeToString(response->Type()));
			}
			return unique_ptr_cast<QuackMessage, TARGET>(std::move(response));
		}
	}

	//! Acknowledge a result the client will not fetch any further, so the server can drop it.
	//! Best-effort and safe to call from a destructor: never throws, and skips silently when no
	//! cached client is available.
	void AcknowledgeResult(hugeint_t query_uuid) const noexcept;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

	//! The server's database is invalidated, so this connection is a dead end - fail fast on it. Reconnecting
	//! would be worse than useless: an invalidated server cannot run its authentication query either, so it
	//! rejects every reconnect with a misleading "Authentication failed".
	void MarkServerInvalidated() const;
	bool ServerInvalidated() const {
		return server_invalidated;
	}

private:
	friend class QuackClient;

	void StartHeartbeat();
	void SendHeartbeat();
	unique_ptr<QuackClient> TakeClient(optional_ptr<ClientContext> context) const;

	DatabaseInstance &db;
	QuackUri uri;
	mutable string connection_id;
	//! Lease timeout accepted by the server during the connection handshake.
	idx_t heartbeat_timeout_seconds;
	//! Retained so the session can be re-established after the server forgets it
	string token;
	string client_id;
	mutable mutex lock;
	mutable atomic<bool> server_invalidated {false};
	//! Serializes re-handshakes so a herd of failing requests performs one reconnect
	mutable mutex reconnect_lock;
	//! Bounds cached_clients: each cached client holds a persistent socket that pins a server
	//! connection slot, so an unbounded cache would let one attach starve the server's budget.
	idx_t max_connections_cached;
	mutable vector<unique_ptr<QuackClient>> cached_clients;

	QuackPeriodicWorker heartbeat;
};

class HttpsQuackClient : public QuackClient {
public:
	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

	string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) override;
	void PrepareTeardownRequest() override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;
	//! POST bytes assuming request_mutex is already held.
	string PostRawLocked(const_data_ptr_t data, idx_t size);
	//! Lazily initialise http_params (context-aware) and attach request_logger; assumes request_mutex is held.
	void EnsureHttpParams(optional_ptr<ClientContext> context);

private:
	unique_ptr<HTTPParams> http_params;
	//! Persistent keep-alive HTTP client: reused across requests so the TCP connection (and its
	//! warm congestion window) survives between POSTs; replaced by the retry path on dead sockets,
	//! and dropped here when a request fails, since it may be left mid-exchange.
	unique_ptr<HTTPClient> http_client;
	//! Set by PrepareTeardownRequest(): cap the transport timeout and drop retries so the final
	//! best-effort DisconnectMessage cannot hang the destructor on a dead peer.
	bool teardown_request = false;
};

//! HTTPS client that trusts exactly one server certificate, the one whose SHA-256 fingerprint the URI
//! pins (ssl_fingerprint), instead of the CA chain. Self-signed quack_serve certificates never verify
//! against a CA, so this is how a client authenticates such a server. Talks httplib+OpenSSL directly:
//! the httpfs transport has no pinning hook.
class PinnedHttpsQuackClient : public QuackClient {
public:
	PinnedHttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~PinnedHttpsQuackClient() override;

	string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) override;
	void PrepareTeardownRequest() override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;
	//! POST bytes assuming request_mutex is already held.
	string PostRawLocked(const_data_ptr_t data, idx_t size);

private:
	unique_ptr<duckdb_httplib_openssl::Client> https_client;
};

} // namespace duckdb
