#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include <algorithm>

#include "quack_scan.hpp"
#include "quack_filter_sql.hpp"
#include "quack_client.hpp"
#include "include/storage/quack_catalog.hpp"
#include "storage/quack_transaction.hpp"

#include <queue>
namespace duckdb {

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
	bind_data->client_connection = QuackClient::ConnectToServer(context, server_uri, token);
	auto &client_connection = *bind_data->client_connection;

	auto client_wrapper = client_connection.GetClient(context);
	auto &client = client_wrapper->GetClient();

	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query));

	return_types = bind_response->Types();
	names = bind_response->Names();

	bind_data->remote_query = query;
	bind_data->column_names = names;
	bind_data->column_types = return_types;
	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->result_uuid = bind_response->ResultUUID();
	bind_data->owns_pending_result = bind_data->needs_more_fetch;
	bind_data->has_unconsumed_bind_result = true;

	return bind_data;
}

QuackCatalog &GetQuackCatalog(ClientContext &context, Value &catalog_name) {
	if (catalog_name.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = catalog_name.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "quack") {
		throw BinderException("Attached database \"%s\" does not refer to a RPC database", db_name);
	}
	return catalog.Cast<QuackCatalog>();
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

	auto &catalog = GetQuackCatalog(context, input.inputs[0]);
	if (use_transaction) {
		// start a transaction if "use_transaction" is specified
		auto &transaction = QuackTransaction::Get(context, catalog);
		transaction.ForceStart();
	}

	// TODO some of this stuff below is duplicated af
	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->client_connection = catalog.GetClientConnection();
	auto client_wrapper = bind_data->client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(bind_data->client_connection->ConnectionId(), query));

	return_types = bind_response->Types();
	names = bind_response->Names();

	// new stuff
	bind_data->remote_query = query;
	bind_data->column_names = names;
	bind_data->column_types = return_types;
	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->result_uuid = bind_response->ResultUUID();
	bind_data->owns_pending_result = bind_data->needs_more_fetch;
	bind_data->has_unconsumed_bind_result = true;
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

	unique_ptr<QuackClientWrapper> client_wrapper;
	//! batch_index of the batch that `fetched_results` currently holds chunks from (server-assigned).
	//! Surfaced to DuckDB via get_partition_data so downstream order-preserving operators
	//! (CTAS, COPY TO, INSERT SELECT) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	queue<ChunkResult> results;
	ColumnDataScanState scan_state;
};

struct QuackScanGlobalState : GlobalTableFunctionState {
	explicit QuackScanGlobalState(vector<ColumnIndex> column_ids_p, vector<idx_t> projection_id_p,
	                              vector<ChunkResult> results_p, bool needs_more_fetch_p, hugeint_t result_uuid_p,
	                              shared_ptr<QuackClientConnection> client_connection_p, bool pushdown_applied_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(std::move(column_ids_p)),
	      projection_ids(std::move(projection_id_p)), needs_more_fetch(needs_more_fetch_p), result_uuid(result_uuid_p),
	      client_connection(std::move(client_connection_p)), pushdown_applied(pushdown_applied_p),
	      results(std::move(results_p)) {
	}
	~QuackScanGlobalState() override {
		if (needs_more_fetch && client_connection) {
			// the scan ended before draining the result (e.g. a LIMIT) - let the server drop it
			client_connection->CloseResult(result_uuid);
		}
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	atomic<bool> needs_more_fetch;
	hugeint_t result_uuid;
	shared_ptr<QuackClientConnection> client_connection;
	//! Whether the server already applied projection/filters to this result; fetched chunks
	//! then come back final instead of needing the client-side projection
	bool pushdown_applied;

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}

private:
	mutex lock;
	vector<ChunkResult> results;
};

//! All WHERE conjuncts for the rewritten remote query: the pushed-down table filters plus
//! any expressions consumed at optimization time.
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

//! The columns the scan must output: with filter_prune, projection_ids selects the output
//! subset of column_indexes and filter-only columns stay out of the SELECT list (they are
//! evaluated in the WHERE instead).
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
//! Returns an empty string when the bind-time result can stream as-is: no filters to enforce
//! and either the full width is needed or the data already arrived in the bind-time batch.
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

	// For the catalog path (ATTACH), LookupEntry only prepares without executing
	// to avoid the server-side result being overwritten by subsequent lookups.
	// We execute the query here, right before scanning, so the result is fresh.
	vector<ChunkResult> results;
	bool needs_more_fetch = bind_data.needs_more_fetch;
	bool pushdown_applied = true;
	hugeint_t result_uuid;
	string query;
	bool raw_replay = false;
	if (!bind_data.table_name.empty()) {
		// apply pushdown to the query
		query = BuildPushdownQuery(bind_data, input);
	} else {
		query = BuildByNamePushdownQuery(bind_data, input);
		if (query.empty() && !bind_data.has_unconsumed_bind_result) {
			// the bind-time result was already consumed (a re-executed prepared statement)
			// or never existed (a copied or rewritten bind data) - run the query again
			query = bind_data.remote_query;
			raw_replay = true;
		}
	}
	if (!query.empty()) {
		if (bind_data.table_name.empty() && bind_data.owns_pending_result) {
			// the bind-time result streams the raw relation; drop it in favour of the
			// rewritten query
			bind_data.client_connection->CloseResult(bind_data.result_uuid);
			input.bind_data->CastNoConst<QuackScanBindData>().owns_pending_result = false;
		}
		auto &client_connection = *bind_data.client_connection;
		auto client_wrapper = client_connection.GetClient(context);
		auto &client = client_wrapper->GetClient();
		auto response_message = client.Request<PrepareResponseMessage>(
		    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query));
		// the scan consumes chunks positionally; a server-side schema drift must fail loudly
		// instead of mapping the wrong columns
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
			throw InvalidInputException("quack scan: the server returned a result shaped (%s) for \"%s\", expected "
			                            "(%s)",
			                            type_list(response_message->Types()), query, type_list(expected_types));
		}
		pushdown_applied = !raw_replay;
		needs_more_fetch = response_message->NeedsMoreFetch();
		// fetch the result
		auto chunk_pushdown_type =
		    raw_replay ? ChunkResultPushdownType::REQUIRES_PUSHDOWN : ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED;
		for (auto &chunk_ref : response_message->MutableResults()) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, chunk_pushdown_type);
		}
		result_uuid = response_message->ResultUUID();
	} else {
		pushdown_applied = false;
		for (auto &chunk_ref : bind_data.results) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, ChunkResultPushdownType::REQUIRES_PUSHDOWN);
		}
		result_uuid = bind_data.result_uuid;
		// the scan takes over the bind-time result; its global state now closes it if the
		// scan ends early, and any later execution of this bind data must re-PREPARE
		auto &mutable_bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
		mutable_bind_data.owns_pending_result = false;
		mutable_bind_data.has_unconsumed_bind_result = false;
	}
	// we only multithread if there is more to fetch
	auto projection_ids = input.CanRemoveFilterColumns() ? input.projection_ids : vector<idx_t>();
	return make_uniq<QuackScanGlobalState>(input.column_indexes, std::move(projection_ids), std::move(results),
	                                       needs_more_fetch, result_uuid, bind_data.client_connection,
	                                       pushdown_applied);
}

unique_ptr<LocalTableFunctionState> QuackScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	auto &global_state = global_state_p->Cast<QuackScanGlobalState>();
	auto local_state = make_uniq<QuackScanLocalState>();

	// re-use initial client from bind if possible
	local_state->client_wrapper = bind_data.client_connection->GetClient(context.client);
	auto results = global_state.TryGetResults();
	for (auto &chunk : results) {
		local_state->results.push(std::move(chunk));
	}
	return local_state;
}

static void QuackScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
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
					// chunks hold the full-width relation; apply the projection (and the
					// filter-column pruning, when set) client-side
					auto &projection_ids = global_state.projection_ids;
					auto output_count = projection_ids.empty() ? global_state.column_ids.size() : projection_ids.size();
					for (idx_t i = 0; i < output_count; i++) {
						auto &index = global_state.column_ids[projection_ids.empty() ? i : projection_ids[i]];
						if (index.IsVirtualColumn()) {
							output.data[i].Reference(Value(output.data[i].GetType()), count_t(response_chunk.size()));
							continue;
						}
						auto col_idx = index.GetPrimaryIndex();
						output.data[i].Reference(response_chunk.data[col_idx]);
					}
					output.SetCardinality(response_chunk.size());
				}
				return;
			}
		}

		// if that did not work, we request more results
		if (local_state.results.empty() && global_state.needs_more_fetch) {
			auto &client = local_state.client_wrapper->GetClient();
			auto fetch_response = client.Request<FetchResponseMessage>(
			    context,
			    make_uniq<FetchRequestMessage>(bind_data.client_connection->ConnectionId(), global_state.result_uuid));

			if (fetch_response->MutableResults().empty()) {
				// server is done, we are done
				global_state.needs_more_fetch = false;
				return;
			}
			// Scans that re-PREPAREd with pushdown applied get final chunks back; a scan
			// streaming its bind-time result still needs the client-side projection on
			// every fetched chunk.
			auto pushdown_type = global_state.pushdown_applied ? ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED
			                                                   : ChunkResultPushdownType::REQUIRES_PUSHDOWN;
			// set up buffer for scan in next iteration
			for (auto &chunk : fetch_response->MutableResults()) {
				local_state.results.emplace(chunk->Chunk(), pushdown_type);
			}
			local_state.current_batch_index = fetch_response->BatchIndex();
			continue;
		}
		// we did not have anything cached and then request to the server did not yield anything - we are done
		break;
	}
}

static bool QuackScanPushdownAggregate(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                       const vector<unique_ptr<Expression>> &groups,
                                       const vector<unique_ptr<Expression>> &aggregates, vector<LogicalType> &new_types,
                                       vector<Identifier> &new_names) {
	if (!bind_data_p || groups.empty() || aggregates.empty()) {
		return false;
	}
	auto &bind_data = bind_data_p->Cast<QuackScanBindData>();
	if (!bind_data.table_name.empty() || bind_data.remote_query.empty()) {
		return false;
	}
	auto &column_ids = get.GetColumnIds();

	// render everything before touching any state, so a failed render leaves the scan as-is
	vector<string> select_list;
	vector<string> group_by;
	vector<LogicalType> types;
	vector<string> names;
	case_insensitive_set_t used_names;
	auto unique_name = [&](string base) {
		auto candidate = base;
		for (idx_t i = 2; used_names.find(candidate) != used_names.end(); i++) {
			candidate = base + "_" + to_string(i);
		}
		used_names.insert(candidate);
		return candidate;
	};
	for (idx_t i = 0; i < groups.size(); i++) {
		auto &group = *groups[i];
		auto rendered = RenderComplexFilter(group, column_ids, bind_data.column_names, bind_data.column_types);
		if (rendered.empty()) {
			return false;
		}
		string base = "group_" + to_string(i + 1);
		if (group.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto col = group.Cast<BoundColumnRefExpression>().Binding().column_index.GetIndex();
			base = bind_data.column_names[column_ids[col].GetPrimaryIndex()];
		}
		// constant group keys (leg tags) group as well: unlike a global aggregate, grouping by
		// a constant yields no row for an empty input, matching the aggregate being replaced
		group_by.push_back(to_string(select_list.size() + 1));
		auto name = unique_name(base);
		select_list.push_back(rendered + " AS " + SQLIdentifier::ToString(name));
		types.push_back(group.GetReturnType());
		names.push_back(std::move(name));
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto rendered = RenderAggregateCall(*aggregates[i], column_ids, bind_data.column_names, bind_data.column_types);
		if (rendered.empty()) {
			return false;
		}
		auto name = unique_name("aggregate_" + to_string(i + 1));
		select_list.push_back(rendered + " AS " + SQLIdentifier::ToString(name));
		types.push_back(aggregates[i]->GetReturnType());
		names.push_back(std::move(name));
	}
	// filters already pushed into this scan apply before the aggregation
	vector<string> where_clauses = bind_data.remote_filters;
	if (get.table_filters.HasFilters()) {
		auto filter_clause =
		    BuildFilterWhereClause(get.table_filters, column_ids, bind_data.column_names, bind_data.column_types);
		if (!filter_clause.empty()) {
			where_clauses.push_back(std::move(filter_clause));
		}
	}

	auto query =
	    "SELECT " + StringUtil::Join(select_list, ", ") + StringUtil::Format(" FROM (%s)", bind_data.remote_query);
	if (!where_clauses.empty()) {
		query += " WHERE " + StringUtil::Join(where_clauses, " AND ");
	}
	query += " GROUP BY " + StringUtil::Join(group_by, ", ");

	// commit: the scan now produces the grouped result; the bind-time raw-relation result is
	// not replayable (its pending server result is still closed by the bind data destructor)
	bind_data.remote_query = std::move(query);
	bind_data.column_names = names;
	bind_data.column_types = types;
	bind_data.remote_filters.clear();
	bind_data.results.clear();
	bind_data.has_unconsumed_bind_result = false;
	bind_data.needs_more_fetch = true;
	new_types = std::move(types);
	for (auto &name : names) {
		new_names.emplace_back(std::move(name));
	}
	return true;
}

static bool QuackScanPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	// anything accepted here becomes a required filter the scan must enforce, so acceptance
	// has to equal serializability
	return IsDeparseSafe(expr);
}

static void QuackScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                           vector<unique_ptr<Expression>> &filters) {
	if (!bind_data_p) {
		return;
	}
	auto &bind_data = bind_data_p->Cast<QuackScanBindData>();
	auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < filters.size();) {
		auto &expr = *filters[i];
		// single-column non-throwing expressions go through the regular table-filter path,
		// which keeps them visible for statistics and EXPLAIN; consume only what that path
		// will not take (e.g. fallible functions like json extraction, or predicates over
		// several columns)
		vector<ColumnBinding> bindings;
		ColumnLifetimeAnalyzer::ExtractColumnBindings(expr, bindings);
		bool single_column = !bindings.empty() && std::all_of(bindings.begin(), bindings.end(),
		                                                      [&](const ColumnBinding &b) { return b == bindings[0]; });
		if (expr.IsVolatile() || bindings.empty() || (single_column && !expr.CanThrow())) {
			i++;
			continue;
		}
		auto rendered = RenderComplexFilter(expr, column_ids, bind_data.column_names, bind_data.column_types);
		if (rendered.empty()) {
			i++;
			continue;
		}
		bind_data.remote_filters.push_back(std::move(rendered));
		filters.erase(filters.begin() + NumericCast<int64_t>(i));
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
	if (!bind_data.remote_filters.empty()) {
		result["Remote Filters"] = StringUtil::Join(bind_data.remote_filters, " AND ");
	}
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

TableFunction QuackScanFunction::GetFunction() {
	auto fun = TableFunction("quack_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan, QuackScanBind,
	                         QuackScanInitGlobal, QuackScanInitLocal);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;

	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_bind_info = QuackScanGetBindInfo;
	fun.filter_pushdown = true;
	fun.filter_prune = true;
	fun.pushdown_expression = QuackScanPushdownExpression;
	fun.pushdown_complex_filter = QuackScanPushdownComplexFilter;
	fun.pushdown_aggregate = QuackScanPushdownAggregate;
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
	fun.named_parameters["use_transaction"] = LogicalType::BOOLEAN;
	fun.filter_pushdown = true;
	fun.filter_prune = true;
	fun.pushdown_expression = QuackScanPushdownExpression;
	fun.pushdown_complex_filter = QuackScanPushdownComplexFilter;
	fun.pushdown_aggregate = QuackScanPushdownAggregate;
	return fun;
}

bool QuackCatalog::IsQuackScan(const string &name) {
	return name == "quack_query" || name == "quack_query_by_name";
}

} // namespace duckdb
