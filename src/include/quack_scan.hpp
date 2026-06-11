#pragma once

#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

struct QuackScanBindData : FunctionData {
	~QuackScanBindData() override {
		if (owns_pending_result && client_connection) {
			// the result PREPAREd at bind time was never scanned (e.g. EXPLAIN) - let the
			// server drop it
			client_connection->CloseResult(result_uuid);
		}
	}

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
		result->column_names = column_names;
		result->column_types = column_types;
		return std::move(result);
	}

	string table_name;
	//! The raw query text sent to the server (by-name scans only; empty on the catalog path)
	string remote_query;
	//! Filter expressions consumed at optimization time, already rendered as SQL conjuncts
	//! for the remote WHERE clause
	vector<string> remote_filters;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<unique_ptr<DataChunkWrapper>> results;
	shared_ptr<QuackClientConnection> client_connection;
	optional_ptr<TableCatalogEntry> table_entry;
	bool needs_more_fetch = true;
	hugeint_t result_uuid;
	//! Whether this bind data is responsible for the server-side result it PREPAREd at bind
	//! time; responsibility moves to the scan's global state once scanning starts.
	//! Deliberately not copied: a copy never owns the original's pending result.
	bool owns_pending_result = false;
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
