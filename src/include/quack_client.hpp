#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_uri.hpp"

#include <functional>

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

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
				response_message->Cast<ErrorResponse>().Error().Throw();
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr_cast<QuackMessage, TARGET>(std::move(response_message));
	}

	//! POST already-serialized request bytes and return the raw response body, throwing on transport failure.
	//! Lets an async sender serialize on a producer thread and perform the blocking POST from the ASYNC pool;
	//! pass context=nullptr when called off the execution thread (parameters fall back to the database).
	virtual string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) = 0;

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
	                                                         string client_id = {});

	//! Resolve the effective client_id for a new connection
	static string ResolveClientId(ClientContext &context, optional_ptr<const Value> explicit_value);

	//! Throw unless `client_id` is either empty ("no client_id") or >= 4 characters
	static void ValidateClientId(const string &client_id);

protected:
	//! Resolve the logger for a request: the context (per-query) logger when available, else the db logger.
	Logger &GetRequestLogger(optional_ptr<ClientContext> context);

	mutex request_mutex;
	MemoryStream write_stream;
	DatabaseInstance &db;
	QuackUri uri;
	//! HTTP transport-log logger, stamped at checkout (see SetRequestLogger).
	shared_ptr<Logger> request_logger;

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
	explicit QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
	                               string token_p = {}, string client_id_p = {},
	                               idx_t max_connections_cached = 1);
	~QuackClientConnection();

	void CancelQuery(hugeint_t query_uuid);

	const string &ConnectionId() const {
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}

	//! Get a client (either a cached one, or open a new one if required)
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Re-establish the server session after the server forgot it (e.g. a restart). Returns true
	//! when the connection now holds an id different from stale_connection_id - either because
	//! this call performed the handshake, or because another thread already did. Reconnects are
	//! serialized, so a herd of failing requests performs one handshake.
	bool Reconnect(ClientContext &context, const string &stale_connection_id) const;

	//! Whether an error response says the server no longer knows this session. Matches the raw
	//! message the server sent, not the decorated form Message() renders.
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

	//! Tell the server a pending result will not be fetched any further and can be dropped.
	//! Best-effort and safe to call from a destructor: never throws, and skips silently when no
	//! cached client is available.
	void CloseResult(hugeint_t query_uuid) const noexcept;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

private:
	QuackUri uri;
	mutable string connection_id;
	//! Retained so the session can be re-established after the server forgets it
	string token;
	string client_id;
	mutable mutex lock;
	//! Serializes re-handshakes so a herd of failing requests performs one reconnect
	mutable mutex reconnect_lock;
	//! Bounds cached_clients: each cached client holds a persistent socket that pins a server
	//! connection slot, so an unbounded cache would let one attach starve the server's budget.
	idx_t max_connections_cached;
	mutable vector<unique_ptr<QuackClient>> cached_clients;
};

class HttpsQuackClient : public QuackClient {
public:
	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

	string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) override;

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
};

} // namespace duckdb
