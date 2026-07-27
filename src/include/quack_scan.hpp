#pragma once

#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

struct QuackScanBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanBindData>();
		return other.client_connection->ConnectionId() == client_connection->ConnectionId() &&
		       other.client_connection->ServerURI() == client_connection->ServerURI() &&
		       other.table_name == table_name && other.remote_query == remote_query &&
		       other.remote_filters == remote_filters && other.column_names == column_names &&
		       other.column_types == column_types;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanBindData>();
		result->client_connection = client_connection;
		result->table_name = table_name;
		result->remote_query = remote_query;
		result->remote_filters = remote_filters;
		result->estimated_cardinality = estimated_cardinality;
		result->column_names = column_names;
		result->column_types = column_types;
		return std::move(result);
	}

	string table_name;
	//! The raw query text sent to the server (by-name scans only; empty on the catalog path)
	string remote_query;
	//! Filter SQL captured at optimize time (expression pushdown), shipped in the WHERE clause
	vector<string> remote_filters;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<unique_ptr<DataChunkWrapper>> results;
	shared_ptr<QuackClientConnection> client_connection;
	optional_ptr<TableCatalogEntry> table_entry;
	bool needs_more_fetch = true;
	//! Whether the bind-time result is still unfetched. A rewrite that produces no new query
	//! replays those chunks; once consumed (a re-executed prepared statement) the original query
	//! must be re-issued instead of replaying a result that is no longer there.
	bool has_unconsumed_bind_result = false;
	//! Row estimate the server reported for this query; 0 when it reported none
	idx_t estimated_cardinality = 0;
	hugeint_t query_uuid;
	atomic<bool> completed;

	~QuackScanBindData() override {
		if (!completed && query_uuid != hugeint_t {0, 0}) {
			try {
				client_connection->CancelQuery(query_uuid);
			} catch (...) {
				// server may already be gone
			}
		}
	}
};

class TableFunction;

class QuackScanFunction {
public:
	static TableFunction GetFunction();
};

class QuackScanByNameFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
