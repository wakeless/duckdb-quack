#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include "quack_scan.hpp"
#include "quack_filter_sql.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "quack_client.hpp"
#include "quack_fetch_ahead.hpp"
#include "include/storage/quack_catalog.hpp"
#include "storage/quack_transaction.hpp"

#include <queue>
namespace duckdb {

//! Whether the scan should execute its query eagerly at bind time (the PREPARE response then
//! doubles as the first result batch) or only resolve the schema. View legs bind schema-only:
//! pushdown routinely rewrites their query, which would discard an eagerly executed result.
static bool IsEagerBind(TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("eager");
	if (entry == input.named_parameters.end()) {
		return true;
	}
	if (entry->second.IsNull()) {
		throw InvalidInputException("eager cannot be null");
	}
	return BooleanValue::Get(entry->second);
}

static void CaptureBindResponse(QuackScanBindData &bind_data, PrepareResponseMessage &response, const string &query,
                                bool eager, vector<LogicalType> &return_types, vector<string> &names) {
	return_types = response.Types();
	names = response.Names();
	bind_data.remote_query = query;
	bind_data.column_names = names;
	bind_data.column_types = return_types;
	bind_data.estimated_cardinality = response.EstimatedCardinality();
	if (eager) {
		bind_data.results = std::move(response.MutableResults());
		bind_data.needs_more_fetch = response.NeedsMoreFetch();
		bind_data.query_uuid = response.QueryUUID();
		bind_data.has_unconsumed_bind_result = true;
	} else {
		// Schema-only bind: nothing executed, so there is no result to stream or close. The
		// scan's init issues the (possibly rewritten) query as the only PREPARE.
		bind_data.needs_more_fetch = true;
		bind_data.query_uuid = hugeint_t(0);
		bind_data.has_unconsumed_bind_result = false;
	}
}

static unique_ptr<FunctionData> QuackScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs.empty()) {
		throw InternalException("No input to quack scan?");
	}
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("quack_query URI and query parameters cannot be NULL");
	}

	auto query = input.inputs[1].GetValue<string>();
	auto initial_uri = QuackUri(input.inputs[0].GetValue<string>());

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	if (input.named_parameters.find("disable_ssl") != input.named_parameters.end()) {
		enable_ssl = !input.named_parameters["disable_ssl"].GetValue<bool>();
	}

	auto bind_data = make_uniq<QuackScanBindData>();
	auto server_uri = QuackUri(initial_uri.Uri(), enable_ssl);

	// Resolve auth token: prefer a quack secret scoped to this URI; fall back to the
	// global rpc_default_token setting. Mirrors the logic in QuackCatalog::QuackCatalog.
	string token;
	if (input.named_parameters.find("token") != input.named_parameters.end()) {
		token = input.named_parameters["token"].GetValue<string>();
	}
	auto client_id_entry = input.named_parameters.find("client_id");
	auto client_id = QuackClient::ResolveClientId(
	    context, client_id_entry != input.named_parameters.end() ? &client_id_entry->second : nullptr);
	bind_data->client_connection = QuackClient::ConnectToServer(context, server_uri, token, std::move(client_id));
	auto &client_connection = *bind_data->client_connection;

	auto client_wrapper = client_connection.GetClient(context);
	auto &client = client_wrapper->GetClient();

	auto eager = IsEagerBind(input);
	bind_data->query_uuid = UUID::GenerateRandomUUID();
	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query, bind_data->query_uuid,
	                                              /*prepare_only=*/!eager));
	CaptureBindResponse(*bind_data, *bind_response, query, eager, return_types, names);

	return bind_data;
}

static unique_ptr<FunctionData> QuackScanBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("catalog_name and query parameters cannot be NULL");
	}
	bool use_transaction = false;
	auto entry = input.named_parameters.find("use_transaction");
	if (entry != input.named_parameters.end()) {
		if (entry->second.IsNull()) {
			throw InvalidInputException("use_transaction cannot be null");
		}
		use_transaction = BooleanValue::Get(entry->second);
	}

	auto &catalog = QuackCatalog::GetQuackCatalog(context, input.inputs[0]);
	if (use_transaction) {
		// start a transaction if "use_transaction" is specified
		auto &transaction = QuackTransaction::Get(context, catalog);
		transaction.ForceStart();
	}

	// TODO some of this stuff below is duplicated af
	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->client_connection = catalog.GetClientConnection(context);
	auto client_wrapper = bind_data->client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto eager = IsEagerBind(input);
	bind_data->query_uuid = UUID::GenerateRandomUUID();
	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(bind_data->client_connection->ConnectionId(), query,
	                                              bind_data->query_uuid, /*prepare_only=*/!eager));
	CaptureBindResponse(*bind_data, *bind_response, query, eager, return_types, names);
	return bind_data;
}

enum class ChunkResultPushdownType { REQUIRES_PUSHDOWN, PUSHDOWN_ALREADY_APPLIED };

class ChunkResult {
public:
	explicit ChunkResult(DataChunk &chunk_p, ChunkResultPushdownType pushdown_type_p) : pushdown_type(pushdown_type_p) {
		chunk = make_uniq<DataChunk>();
		chunk->InitializeEmpty(chunk_p.GetTypes());
		chunk->Reference(chunk_p);
	}
	DataChunk &Chunk() {
		return *chunk;
	}
	bool RequiresPushdown() const {
		return pushdown_type == ChunkResultPushdownType::REQUIRES_PUSHDOWN;
	}

private:
	unique_ptr<DataChunk> chunk;
	ChunkResultPushdownType pushdown_type;
};

struct QuackScanLocalState : public LocalTableFunctionState {
	explicit QuackScanLocalState() {
	}
	~QuackScanLocalState() override {
	}

	//! Server-assigned index of the batch currently being drained; surfaced via get_partition_data
	//! so order-preserving operators (CTAS, COPY TO) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	//! This thread's outstanding batch claim; persists across BLOCKED yields until the batch arrives.
	optional_idx fetch_claim;
	//! The stream ended below this thread's claim; no more batches for this thread.
	bool fetch_exhausted = false;

	queue<ChunkResult> results;
	ColumnDataScanState scan_state;
};

struct QuackScanGlobalState : GlobalTableFunctionState {
	explicit QuackScanGlobalState(vector<ColumnIndex> column_ids_p, vector<idx_t> projection_id_p,
	                              vector<ChunkResult> results_p, bool needs_more_fetch_p, hugeint_t query_uuid_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(std::move(column_ids_p)),
	      projection_ids(std::move(projection_id_p)), query_uuid(query_uuid_p), results(std::move(results_p)) {
	}
	~QuackScanGlobalState() override {
		// A scan that stopped early (a LIMIT, or an error upstream) leaves rows unfetched, and the
		// server holds that result until the session ends - every later query on the connection
		// then pays to drain it out of the way. Tell the server to drop it.
		if (!fetcher || !client_connection) {
			return;
		}
		auto abandoned = !fetcher->ServerExhausted();
		// Release the fetcher first: it is holding the pooled clients, and CloseResult needs one.
		fetcher.reset();
		if (abandoned) {
			client_connection->CloseResult(query_uuid);
		}
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	shared_ptr<QuackClientConnection> client_connection;
	//! Whether the server already applied projection/filters to this result; fetched chunks then
	//! arrive final instead of needing the client-side projection
	bool pushdown_applied = false;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	atomic<bool> ack_sent {false};
	hugeint_t query_uuid;
	//! FETCH read-ahead pipeline; set when the PREPARE response signals more batches to fetch.
	shared_ptr<QuackFetcher> fetcher;

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}

private:
	mutex lock;
	vector<ChunkResult> results;
};

//! All WHERE conjuncts for the rewritten remote query: filters captured at optimize time plus
//! the table filters the planner pushed into this scan.
static string BuildWhereClause(const QuackScanBindData &bind_data, const TableFunctionInitInput &input) {
	vector<string> clauses = bind_data.remote_filters;
	if (input.filters && input.filters->HasFilters()) {
		auto filter_clause = BuildFilterWhereClause(*input.filters, input.column_indexes, bind_data.column_names,
		                                            bind_data.column_types);
		if (!filter_clause.empty()) {
			clauses.push_back(std::move(filter_clause));
		}
	}
	return StringUtil::Join(clauses, " AND ");
}

//! The columns the scan must output: with filter_prune, projection_ids selects the output subset
//! of column_indexes and filter-only columns stay out of the SELECT list, since they are
//! evaluated in the WHERE instead.
static vector<ColumnIndex> OutputColumns(const TableFunctionInitInput &input) {
	if (!input.CanRemoveFilterColumns()) {
		return input.column_indexes;
	}
	vector<ColumnIndex> result;
	for (auto &proj_id : input.projection_ids) {
		result.push_back(input.column_indexes[proj_id]);
	}
	return result;
}

static string BuildPushdownQuery(const QuackScanBindData &bind_data, const TableFunctionInitInput &input) {
	string query;

	// Projection: select only the columns DuckDB actually needs in the output.
	auto output_columns = OutputColumns(input);
	if (!output_columns.empty()) {
		for (auto &col_id : output_columns) {
			if (!query.empty()) {
				query += ", ";
			}
			if (col_id.IsVirtualColumn()) {
				auto virtual_column = col_id.GetPrimaryIndex();
				if (virtual_column == COLUMN_IDENTIFIER_EMPTY || virtual_column == COLUMN_IDENTIFIER_ROW_ID) {
					query += "NULL::BIGINT";
				} else {
					throw InternalException("Unsupported virtual column index");
				}
			} else {
				query += "#" + to_string(col_id.GetPrimaryIndex() + 1);
			}
		}
		query = "SELECT " + query + " ";
	}
	query += StringUtil::Format("FROM %s", SQLIdentifier(bind_data.table_name));
	auto where_clause = BuildWhereClause(bind_data, input);
	if (!where_clause.empty()) {
		query += " WHERE " + where_clause;
	}

	return query;
}

//! Rebuild the remote SQL for a by-name scan with projection/filters applied server-side.
//! Returns an empty string when the bind-time result can stream as-is: no filters to enforce and
//! either the full width is needed or the data already arrived in the bind-time batch.
static string BuildByNamePushdownQuery(const QuackScanBindData &bind_data, const TableFunctionInitInput &input) {
	auto where_clause = BuildWhereClause(bind_data, input);
	auto output_columns = OutputColumns(input);
	bool narrowing = output_columns.size() < bind_data.column_types.size();
	for (auto &col_id : output_columns) {
		if (col_id.IsVirtualColumn()) {
			narrowing = true;
		}
	}
	// projection alone only saves wire time if the server still holds undelivered batches
	if (where_clause.empty() && !(narrowing && bind_data.needs_more_fetch)) {
		return string();
	}
	vector<string> select_list;
	for (auto &col_id : output_columns) {
		if (col_id.IsVirtualColumn()) {
			auto virtual_column = col_id.GetPrimaryIndex();
			if (virtual_column == COLUMN_IDENTIFIER_EMPTY || virtual_column == COLUMN_IDENTIFIER_ROW_ID) {
				select_list.push_back("NULL::BIGINT");
			} else {
				throw InternalException("Unsupported virtual column index");
			}
		} else {
			select_list.push_back(SQLIdentifier::ToString(bind_data.column_names[col_id.GetPrimaryIndex()]));
		}
	}
	auto query = "SELECT " + (select_list.empty() ? string("*") : StringUtil::Join(select_list, ", ")) +
	             StringUtil::Format(" FROM (%s)", bind_data.remote_query);
	if (!where_clause.empty()) {
		query += " WHERE " + where_clause;
	}
	return query;
}

unique_ptr<GlobalTableFunctionState> QuackScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();

	vector<ChunkResult> results;
	bool needs_more_fetch = bind_data.needs_more_fetch;
	bool pushdown_applied = true;
	hugeint_t query_uuid;
	string query;
	bool raw_replay = false;
	if (!bind_data.table_name.empty()) {
		query = BuildPushdownQuery(bind_data, input);
	} else {
		query = BuildByNamePushdownQuery(bind_data, input);
		if (query.empty() && !bind_data.has_unconsumed_bind_result) {
			// The bind-time result was already consumed (a re-executed prepared statement) or
			// never existed (a schema-only bind): run the original query again.
			query = bind_data.remote_query;
			raw_replay = true;
		}
	}
	if (!query.empty()) {
		auto &mutable_bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
		if (bind_data.table_name.empty() && bind_data.has_unconsumed_bind_result && bind_data.needs_more_fetch) {
			// The bind-time result streams the raw relation; drop it in favour of the rewrite
			// rather than leaving it pending on the connection.
			bind_data.client_connection->CloseResult(bind_data.query_uuid);
			mutable_bind_data.has_unconsumed_bind_result = false;
		}
		auto &client_connection = *bind_data.client_connection;
		auto client_wrapper = client_connection.GetClient(context);
		auto &client = client_wrapper->GetClient();
		query_uuid = UUID::GenerateRandomUUID();
		auto response_message = client.Request<PrepareResponseMessage>(
		    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query, query_uuid));
		// The scan consumes chunks positionally, so a server-side schema drift must fail loudly
		// instead of mapping the wrong columns.
		vector<LogicalType> expected_types;
		if (raw_replay) {
			expected_types = bind_data.column_types;
		} else {
			for (auto &col_id : OutputColumns(input)) {
				expected_types.push_back(col_id.IsVirtualColumn() ? LogicalType::BIGINT
				                                                  : bind_data.column_types[col_id.GetPrimaryIndex()]);
			}
		}
		if (response_message->Types() != expected_types) {
			auto type_list = [](const vector<LogicalType> &types) {
				return StringUtil::Join(types, types.size(), ", ",
				                        [](const LogicalType &type) { return type.ToString(); });
			};
			throw InvalidInputException("quack scan: the server returned a result shaped (%s) for \"%s\", expected (%s)",
			                            type_list(response_message->Types()), query, type_list(expected_types));
		}
		pushdown_applied = !raw_replay;
		needs_more_fetch = response_message->NeedsMoreFetch();
		auto chunk_pushdown_type =
		    raw_replay ? ChunkResultPushdownType::REQUIRES_PUSHDOWN : ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED;
		for (auto &chunk_ref : response_message->MutableResults()) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, chunk_pushdown_type);
		}
	} else {
		pushdown_applied = false;
		for (auto &chunk_ref : bind_data.results) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, ChunkResultPushdownType::REQUIRES_PUSHDOWN);
		}
		query_uuid = bind_data.query_uuid;
		// The scan takes over the bind-time result; a later execution of this bind data must
		// re-issue the query rather than replay chunks that have been handed out.
		input.bind_data->CastNoConst<QuackScanBindData>().has_unconsumed_bind_result = false;
	}
	// we only multithread if there is more to fetch
	auto global_state = make_uniq<QuackScanGlobalState>(input.column_indexes, input.projection_ids, std::move(results),
	                                                    needs_more_fetch, query_uuid);
	global_state->client_connection = bind_data.client_connection;
	global_state->pushdown_applied = pushdown_applied;
	if (needs_more_fetch) {
		// start pipelining FETCH requests on the ASYNC pool before the first scan call
		global_state->fetcher = make_shared_ptr<QuackFetcher>(context, *bind_data.client_connection, query_uuid,
		                                                      QuackFetcher::GetReadAheadDepth(context));
	}
	return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> QuackScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	auto &global_state = global_state_p->Cast<QuackScanGlobalState>();
	auto local_state = make_uniq<QuackScanLocalState>();

	auto results = global_state.TryGetResults();
	for (auto &chunk : results) {
		local_state->results.push(std::move(chunk));
	}
	return local_state;
}

static bool ReconnectsEnabled(ClientContext &context) {
	Value val;
	if (!context.TryGetCurrentSetting("quack_enable_reconnects", val)) {
		return false;
	}
	return val.GetValue<bool>();
}

static void SendAcknowledgement(ClientContext &context, QuackScanGlobalState &global_state,
                                QuackScanBindData &bind_data) {
	if (!ReconnectsEnabled(context)) {
		return;
	}
	if (global_state.ack_sent.exchange(true)) {
		return;
	}
	try {
		auto client_wrapper = bind_data.client_connection->GetClient(context);
		auto &client = client_wrapper->GetClient();
		client.Request<SuccessResponse>(context,
		                                make_uniq<AcknowledgementMessage>(bind_data.client_connection->ConnectionId()));
	} catch (const std::exception &e) {
		// The query is already complete, so we swallow the failure rather than fail the query
		DUCKDB_LOG_WARNING(
		    context, StringUtil::Format("Quack: acknowledgement failed and was ignored (query already completed): %s",
		                                e.what()));
	}
}

static void QuackScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
	auto &global_state = input.global_state->Cast<QuackScanGlobalState>();
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();

	while (true) {
		// first we try to scan from our local results buffer if we have any
		while (!local_state.results.empty()) {
			auto chunk = std::move(local_state.results.front());
			local_state.results.pop();

			auto &response_chunk = chunk.Chunk();
			if (response_chunk.size() > 0) {
				if (!chunk.RequiresPushdown()) {
					output.Reference(response_chunk);
				} else {
					for (idx_t i = 0; i < global_state.column_ids.size(); i++) {
						auto &index = global_state.column_ids[i];
						if (index.IsVirtualColumn()) {
							// TODO
							output.data[i].Reference(Value(output.data[i].GetType()), count_t(response_chunk.size()));
							return;
						}
						auto col_idx = index.GetPrimaryIndex();
						output.data[i].Reference(response_chunk.data[col_idx]);
					}
					output.SetCardinality(response_chunk.size());
				}
				return;
			}
		}

		// if that did not work, we consume this thread's claimed batch from the fetcher
		if (local_state.results.empty() && global_state.fetcher && !local_state.fetch_exhausted) {
			idx_t batch_index;
			vector<unique_ptr<DataChunk>> chunks;
			switch (global_state.fetcher->GetBatch(context, input, local_state.fetch_claim, batch_index, chunks)) {
			case QuackFetchResult::BATCH: {
				// Tag fetched chunks like the initial batch (see QuackScanInitGlobal): a result the
				// server projected arrives final, one streaming the raw relation still needs the
				// client-side projection.
				auto fetched_pushdown_type = global_state.pushdown_applied
				                                 ? ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED
				                                 : ChunkResultPushdownType::REQUIRES_PUSHDOWN;
				for (auto &chunk : chunks) {
					local_state.results.emplace(*chunk, fetched_pushdown_type);
				}
				local_state.current_batch_index = batch_index;
				continue;
			}
			case QuackFetchResult::FINISHED:
				// server is done, this thread is done
				local_state.fetch_exhausted = true;
				bind_data.completed = true;
				// FINISHED is only observable once the fetcher drained every in-flight FETCH,
				// so the acknowledgement is guaranteed to be the query's last message
				SendAcknowledgement(context, global_state, bind_data);
				return;
			case QuackFetchResult::BLOCKED:
				return;
			}
		}
		// we did not have anything cached and then request to the server did not yield anything - we are done
		bind_data.completed = true;
		SendAcknowledgement(context, global_state, bind_data);
		break;
	}
}

static OperatorPartitionData QuackScanGetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();
	// If we haven't received a batch yet, fall back to 0 so downstream doesn't choke; the
	// planner only calls this after QuackScan has returned rows, by which point the current
	// batch index is always set.
	auto idx = local_state.current_batch_index.IsValid() ? local_state.current_batch_index.GetIndex() : 0;
	return OperatorPartitionData(idx);
}

InsertionOrderPreservingMap<string> QuackScanToString(TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	InsertionOrderPreservingMap<string> result;
	result["Server"] = bind_data.client_connection->ServerURI().Uri();
	return result;
}

void QuackScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                        const TableFunction &function) {
	throw NotImplementedException("Quack scans cannot be serialized (yet?)");
}

unique_ptr<FunctionData> QuackScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("Quack scans cannot be deserialized (yet?)");
}

BindInfo QuackScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->CastNoConst<QuackScanBindData>();
	if (bind_data.table_entry) {
		return BindInfo(*bind_data.table_entry);
	}
	return BindInfo(ScanType::EXTERNAL);
}

static unique_ptr<NodeStatistics> QuackScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	if (!bind_data_p) {
		return nullptr;
	}
	auto &bind_data = bind_data_p->Cast<QuackScanBindData>();
	if (bind_data.estimated_cardinality > 0) {
		return make_uniq<NodeStatistics>(bind_data.estimated_cardinality);
	}
	// No server estimate (an eager bind, or an older server): assume the configured cardinality
	// so the optimizer does not size the remote relation at the 1-row default.
	Value assumed;
	if (context.TryGetCurrentSetting("quack_assumed_scan_cardinality", assumed)) {
		auto value = assumed.GetValue<uint64_t>();
		if (value > 0) {
			return make_uniq<NodeStatistics>(value);
		}
	}
	return nullptr;
}

TableFunction QuackScanFunction::GetFunction() {
	auto fun = TableFunction("quack_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan, QuackScanBind,
	                         QuackScanInitGlobal, QuackScanInitLocal);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;
	fun.named_parameters["client_id"] = LogicalType::VARCHAR;
	fun.named_parameters["eager"] = LogicalType::BOOLEAN;

	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_bind_info = QuackScanGetBindInfo;
	fun.cardinality = QuackScanCardinality;
	fun.filter_pushdown = true;
	fun.filter_prune = true;
	return fun;
}

TableFunction QuackScanByNameFunction::GetFunction() {
	auto fun = TableFunction("quack_query_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan,
	                         QuackScanBindCatalogName, QuackScanInitGlobal, QuackScanInitLocal);
	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_bind_info = QuackScanGetBindInfo;
	fun.cardinality = QuackScanCardinality;
	fun.named_parameters["use_transaction"] = LogicalType::BOOLEAN;
	fun.named_parameters["eager"] = LogicalType::BOOLEAN;
	fun.filter_pushdown = true;
	fun.filter_prune = true;
	return fun;
}

bool QuackCatalog::IsQuackScan(const string &name) {
	return name == "quack_query" || name == "quack_query_by_name";
}

} // namespace duckdb
