#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/column_index.hpp"

namespace duckdb {

class Expression;
class TableFilterSet;

//! Render the table filters pushed into a quack scan as a SQL WHERE clause the server can
//! re-parse. Returns an empty string when nothing needs shipping. Optional (advisory) filters
//! are skipped; a required filter that cannot be rendered throws, since the planner has already
//! removed it from the plan and the scan must enforce it.
string BuildFilterWhereClause(const TableFilterSet &filters, const vector<ColumnIndex> &column_indexes,
                              const vector<string> &column_names, const vector<LogicalType> &column_types);

//! Whether deparsing this expression via ToString() yields SQL that round-trips through the
//! server's parser: column references, constants, comparisons, conjunctions, IN / IS NULL / NOT,
//! casts and an allowlist of scalar functions.
bool IsDeparseSafe(const Expression &expr);

} // namespace duckdb
