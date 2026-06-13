#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_uri.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <functional>

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

//! The resolved result schema of a remote query (the names and types its bind produces).
struct QuackResultSchema {
	vector<string> names;
	vector<LogicalType> types;
};

class QuackClient {
public:
	explicit QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p);
	virtual ~QuackClient();

	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		auto response_message = RawRequest(context, std::move(request_message));
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

	//! Send a request and return the raw response, including error responses
	unique_ptr<QuackMessage> RawRequest(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		return RequestInternal(context, std::move(request_message));
	}

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token);

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	DatabaseInstance &db;
	QuackUri uri;

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
	                               string token_p, idx_t max_connections_cached = 1);
	~QuackClientConnection();

	string ConnectionId() const {
		lock_guard<mutex> guard(lock);
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}

	//! Get a client (either a cached one, or open a new one if required)
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;
	//! Tell the server a pending result will not be fetched any further and can be dropped.
	//! Best-effort and safe to call from destructors: never throws, and skips silently when
	//! no cached client is available.
	void CloseResult(hugeint_t result_uuid) const noexcept;

	//! Re-establish the server session after the server forgot it (e.g. a restart). Returns
	//! true when the connection now holds an id that differs from stale_connection_id -
	//! either because this call performed the handshake or because another thread already
	//! did. Reconnects are serialized; concurrent callers observe the refreshed id.
	bool Reconnect(ClientContext &context, const string &stale_connection_id) const;

	//! Whether an error response indicates the server no longer knows this session. Matches
	//! the typed code, falling back to the message for servers that predate it.
	static bool IsStaleSessionError(const ErrorResponse &error) {
		return error.ErrorCode() == ErrorResponse::CONNECTION_NOT_FOUND ||
		       error.ErrorMessage() == "Invalid connection id";
	}

	//! Resolve a remote query's result schema, reusing a cached result for the connection's
	//! lifetime so repeated binds of the same query (e.g. a view referenced by many dashboard
	//! queries) skip the cross-region round-trip. On a miss, resolves via a schema-only PREPARE
	//! (reconnecting if the session was lost) and caches it. ttl_seconds bounds staleness;
	//! ttl_seconds <= 0 disables the cache (always re-resolve, never store).
	QuackResultSchema ResolveSchema(ClientContext &context, const string &query, int64_t ttl_seconds) const;
	//! Drop all cached schemas (on catalog refresh, or when a cached schema is found stale).
	void ClearSchemaCache() const;

	//! Send a request built against the current connection id; when the server reports the
	//! session is gone, re-handshake once and resend. Only safe for requests that do not
	//! depend on server-side session state (a fresh PREPARE, a transaction-opening BEGIN);
	//! mid-result fetches, appends and commits must not go through this path.
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

private:
	QuackUri uri;
	mutable string connection_id;
	//! Retained for re-handshakes after a server restart
	string token;
	mutable mutex lock;
	//! Serializes re-handshakes so a herd of failing requests performs one reconnect
	mutable mutex reconnect_lock;
	mutable vector<unique_ptr<QuackClient>> cached_clients;
	idx_t max_connections_cached;

	struct CachedSchema {
		QuackResultSchema schema;
		timestamp_t resolved_at;
	};
	mutable mutex schema_cache_lock;
	mutable unordered_map<string, CachedSchema> schema_cache;
};

class HttpsQuackClient : public QuackClient {
public:
	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;

private:
	unique_ptr<HTTPParams> http_params;
};

} // namespace duckdb
