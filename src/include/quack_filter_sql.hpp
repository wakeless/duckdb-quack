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

//! Render a filter expression whose column references still point at the scan's projection
//! list (as handed to pushdown_complex_filter) as SQL. Returns an empty string when the
//! expression cannot be rendered; the caller then leaves the filter in the plan.
string RenderComplexFilter(const Expression &expr, const vector<ColumnIndex> &column_ids,
                           const vector<string> &column_names, const vector<LogicalType> &column_types);

//! Render an aggregate call (column references as above) as SQL, e.g. "count(*)" or
//! "sum(DISTINCT \"val\")". Returns an empty string when the call cannot be reproduced.
string RenderAggregateCall(const Expression &aggregate, const vector<ColumnIndex> &column_ids,
                           const vector<string> &column_names, const vector<LogicalType> &column_types);

} // namespace duckdb
