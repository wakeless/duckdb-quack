#include "storage/quack_optimizer.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

struct QuackOperatorInfo {
	vector<reference<LogicalGet>> scans;
	idx_t insert_count = 0;
};

struct QuackOperators {
	// map of connection id -> operator info
	unordered_map<string, QuackOperatorInfo> op_info;
};

void GatherQuackScans(LogicalOperator &op, QuackOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (QuackCatalog::IsQuackScan(table_scan.name.GetIdentifierName())) {
			// add a quack scan
			auto &bind_data = get.bind_data->Cast<QuackScanBindData>();
			auto connection_id = bind_data.client_connection->ConnectionId();
			result.op_info[connection_id].scans.push_back(get);
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
		auto &insert = op.Cast<LogicalCreateTable>();
		auto &catalog = insert.schema.ParentCatalog();
		if (catalog.GetCatalogType() == "quack") {
			auto &quack_catalog = catalog.Cast<QuackCatalog>();
			auto connection_id = quack_catalog.GetConnectionId();
			result.op_info[connection_id].insert_count += 1;
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_INSERT) {
		auto &insert = op.Cast<LogicalInsert>();
		auto &catalog = insert.table.ParentCatalog();
		if (catalog.GetCatalogType() == "quack") {
			auto &quack_catalog = catalog.Cast<QuackCatalog>();
			auto connection_id = quack_catalog.GetConnectionId();
			result.op_info[connection_id].insert_count += 1;
		}
	}
	// recurse into children
	for (auto &child : op.children) {
		GatherQuackScans(*child, result);
	}
}

void QuackOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	QuackOperators operators;
	GatherQuackScans(*plan, operators);
	for (auto &entry : operators.op_info) {
		auto &op_info = entry.second;
		// Several scans on one connection are fine: the server keeps a result per scan. A scan feeding an
		// INSERT or CTAS on the same connection is not. The INSERT statement holds the server's DuckDB
		// connection while it waits for the client's batches, so the scan's statement would queue behind
		// it for rows that can only come out of that scan.
		if (op_info.insert_count == 0 || op_info.scans.empty()) {
			continue;
		}
		throw NotImplementedException(
		    "A streaming scan feeding a CTAS / insert on the same connection is not currently supported");
	}
}

} // namespace duckdb
