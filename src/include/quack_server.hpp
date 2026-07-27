#pragma once

#include <thread>

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/error_data.hpp"
#include <map>

#include "quack_uri.hpp"

#include "httplib.hpp" // TODO forward declare

namespace duckdb {

class ClientContext;
class QuackMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;
class EncryptionState;
class QuackDataStream;
class ErrorData;

enum class QuackQueryState : uint8_t { IDLE, ACTIVE, FINISHED, CANCELLED, QUACK_ERROR };

//! An insert stream + its driver thread, taken off a connection so finish/join can run unlocked.
struct DetachedInsertStream {
	shared_ptr<QuackDataStream> stream;
	std::thread thread;
	string id;
	//! Finish + join + deregister; returns any INSERT error. Call WITHOUT a lock held.
	ErrorData FinishAndJoin();
	//! Roll the INSERT back (SetError) + join + deregister. Call WITHOUT a lock held.
	void AbortAndJoin(const string &reason);
};

//! Server-side state for the INSERT a client is currently driving via a SEND_DATA stream on this
//! connection (one at a time). `lock` is held only briefly — never across a Push or a join.
struct QuackInsertState {
	annotated_mutex lock;
	shared_ptr<QuackDataStream> stream DUCKDB_GUARDED_BY(lock);
	std::thread thread DUCKDB_GUARDED_BY(lock);
	string stream_id DUCKDB_GUARDED_BY(lock);
	//! Dead-range markers that arrived before the stream-creating data message, tagged with the stream
	//! they belong to; applied once that stream is created.
	string pending_marker_stream_id DUCKDB_GUARDED_BY(lock);
	vector<std::pair<idx_t, idx_t>> pending_dead_ranges DUCKDB_GUARDED_BY(lock);

	//! Take the active stream/thread/id off (briefly under the lock) for an unlocked finish/abort.
	DetachedInsertStream Detach();
	//! Detach only if the active stream is unrelated to `msg_stream_id` (else return an empty detach).
	DetachedInsertStream DetachIfUnrelated(const string &msg_stream_id);
	//! Detach + drain (`watermark`) + finish + join; returns any INSERT error. Empty if none active.
	ErrorData Finalize(optional_idx watermark = optional_idx());
	//! Active stream if it matches `sid` (caller pushes the range), else buffer the range and return null.
	shared_ptr<QuackDataStream> StreamForDeadRangeOrBuffer(const string &sid, idx_t lo, idx_t hi);
};

//! A result a client has PREPAREd but not yet fully fetched. At most one result per connection
//! holds a `live` streaming result (the underlying DuckDB connection supports only one); when
//! another query needs the connection, the live result is drained into `buffered` (a
//! buffer-managed collection that can spill to disk) and served from there.
struct QuackPendingResult {
	//! Live streaming result (exclusive with `buffered`)
	unique_ptr<QueryResult> live;
	//! Result drained out of the way of a later query; remaining batches are served from here
	unique_ptr<ColumnDataCollection> buffered;
	ColumnDataScanState buffered_scan;
	//! Error raised while draining `live`; reported on the next FETCH of this result
	ErrorData error;
	//! Monotonic counter assigned per FETCH batch, per result — order-preserving parallel scans
	//! need indices that do not interleave with another concurrent result's
	idx_t next_batch_index = 1;
	//! Set once every row has been served: the prepare_count at exhaustion time (see cleanup)
	optional_idx exhausted_at;
};

struct QuackConnection {
	explicit QuackConnection(string session_id_p);
	~QuackConnection();

	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	//! Pending results keyed by the client-supplied query UUID. Keyed rather than single so a
	//! query needing two remote scans on one connection can run.
	map<hugeint_t, QuackPendingResult> pending_results;
	//! Number of PREPAREs handled on this connection; drives exhausted-result cleanup
	idx_t prepare_count = 0;
	//! Current query UUID
	hugeint_t query_uuid;
	string session_id;

	//! Stable per-client reconnect key: HMAC-SHA256(server_hmac_key, client_id). Intentionally excludes
	//! session_id so it stays identical across (re)connections for the same client_id. Empty if no client_id.
	string client_id_hash;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};

	//! The INSERT this connection is currently driving via a client SEND_DATA stream (one at a time).
	QuackInsertState insert;
};

struct QuackConnectionSnapshot {
	string server_id;
	string session_id;
	string client_id_hash;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
};

enum class QuackServerState { UNINITIALIZED, WAITING_TO_START, RUNNING, CLOSED };

class QuackServer {
public:
	explicit QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);
	virtual ~QuackServer();

	//! Stop accepting new connections (close the listener socket) without
	//! joining listener threads. Safe to call from a request-handler thread —
	//! does not wait on httplib's task-queue, which would deadlock when the
	//! caller is itself a worker.
	virtual void StopAccepting() {};

	//! Synchronously stop accepting connections and join the listener threads.
	//! Must NOT be called from a worker / request-handler thread; httplib's
	//! listen-loop teardown joins all workers, which would deadlock.
	virtual void Close() {};

	shared_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id, const string &client_id_hash = {});
	bool DisconnectConnection(const string &session_id);
	// TODO need something to destroy connections

	string GenerateSessionId();

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	vector<QuackConnectionSnapshot> GetActiveConnectionSnap();

	const string &Token() {
		return token;
	}

	const QuackUri &ListenUri() const {
		return uri;
	}

	idx_t ActiveConnectionCount() {
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		return active_connections.size();
	}

	//! Number of client requests this server has served
	idx_t RequestCount() const {
		return request_count;
	}
	//! Number of client transport connections this server has accepted. Compared against
	//! RequestCount() this shows whether clients hold their connections open across requests.
	idx_t ClientConnectionCount() const {
		return client_connection_count;
	}

protected:
	//! Account for a request arriving over the client transport connection identified by
	//! `connection_key` (for HTTP, the client's remote port).
	void RecordRequest(int connection_key);

	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	unique_ptr<QuackMessage> HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

protected:
	std::vector<std::thread> listen_threads;

	weak_ptr<DatabaseInstance> db_ptr;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

	atomic<idx_t> request_count {0};
	atomic<idx_t> client_connection_count {0};

	QuackUri uri;

private:
	string token;
	//! Per-server random key that seeds the HMAC for client_id_hash.
	string server_hmac_key;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);

	void StopAccepting() override;
	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *server, const string &listen_host, uint16_t listen_port);

	unique_ptr<QuackMessage> ReadMessage(MemoryStream &read_stream);

	unique_ptr<duckdb_httplib::Server> server;
	mutex state_lock;
	atomic<QuackServerState> server_state {QuackServerState::UNINITIALIZED};
};

} // namespace duckdb
