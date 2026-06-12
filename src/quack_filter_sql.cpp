#include "quack_filter_sql.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

static bool IsComparisonOperator(const string &name) {
	return name == "=" || name == "==" || name == "!=" || name == "<>" || name == "<" || name == "<=" || name == ">" ||
	       name == ">=";
}

static bool IsAllowedScalarFunction(const string &name) {
	static const char *allowed[] = {"struct_extract",
	                                "struct_extract_at",
	                                "json_extract",
	                                "json_extract_string",
	                                "lower",
	                                "upper",
	                                "length",
	                                "contains",
	                                "starts_with",
	                                "prefix",
	                                "~~",
	                                "!~~"};
	for (auto &entry : allowed) {
		if (StringUtil::CIEquals(entry, name)) {
			return true;
		}
	}
	return false;
}

bool IsDeparseSafe(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		// the alias carries the remote column name (column refs are offered to
		// pushdown_expression before being rewritten into references); an unnamed node
		// would deparse to a positional "#N" that means nothing to the server
		if (expr.GetAlias().empty()) {
			return false;
		}
		break;
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_CAST:
		break;
	case ExpressionClass::BOUND_OPERATOR:
		switch (expr.GetExpressionType()) {
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
		case ExpressionType::OPERATOR_NOT:
			break;
		default:
			return false;
		}
		break;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (TableFilterFunctions::IsTableFilterFunction(func.Function())) {
			// internal filter machinery (optional / dynamic / bloom ...) is not real SQL
			return false;
		}
		auto &name = func.Function().GetName().GetIdentifierName();
		if (func.IsOperator() && IsComparisonOperator(name)) {
			break;
		}
		if (!IsAllowedScalarFunction(name)) {
			return false;
		}
		break;
	}
	default:
		return false;
	}
	bool safe = true;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { safe = safe && IsDeparseSafe(child); });
	return safe;
}

//! GetName() prefers an expression's alias over its rendering, and several ToString
//! implementations compose via GetName() - a leftover alias on an inner node would replace
//! valid SQL with arbitrary text. Column references keep theirs: it is the column name.
static void StripNonColumnAliases(Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		expr.ClearAlias();
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { StripNonColumnAliases(child); });
}

static void RenderFilterExpression(unique_ptr<Expression> expr, vector<string> &clauses) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		// repeated pushes against one column are pre-ANDed; handle each conjunct on its own so
		// an advisory conjunct cannot drag a required one down with it
		auto &conj = expr->Cast<BoundConjunctionExpression>();
		for (auto &child : conj.GetChildrenMutable()) {
			RenderFilterExpression(std::move(child), clauses);
		}
		return;
	}
	if (ExpressionFilter::IsRootOptionalExpression(*expr)) {
		// advisory only (zonemap duplicates, dynamic join filters, ...) - the plan still
		// enforces these elsewhere, so they are safe to leave out of the remote query
		return;
	}
	if (!IsDeparseSafe(*expr)) {
		throw InternalException("quack filter pushdown cannot serialize required filter: %s", expr->ToString());
	}
	StripNonColumnAliases(*expr);
	clauses.push_back(expr->ToString());
}

string RenderComplexFilter(const Expression &expr, const vector<ColumnIndex> &column_ids,
                           const vector<string> &column_names, const vector<LogicalType> &column_types) {
	auto copy = expr.Copy();
	// rewrite each column reference into a name-carrying node the server understands
	bool resolved = true;
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    copy, [&](BoundColumnRefExpression &col_ref, unique_ptr<Expression> &node) {
		    auto proj_idx = col_ref.Binding().column_index;
		    if (proj_idx >= column_ids.size()) {
			    resolved = false;
			    return;
		    }
		    auto &col_index = column_ids[proj_idx];
		    if (col_index.IsVirtualColumn() || col_index.GetPrimaryIndex() >= column_names.size()) {
			    resolved = false;
			    return;
		    }
		    auto col_id = col_index.GetPrimaryIndex();
		    node = make_uniq<BoundReferenceExpression>(Identifier(SQLIdentifier::ToString(column_names[col_id])),
		                                               column_types[col_id], 0ULL);
	    });
	if (!resolved || !IsDeparseSafe(*copy)) {
		return string();
	}
	StripNonColumnAliases(*copy);
	return copy->ToString();
}

string RenderAggregateCall(const Expression &aggregate_p, const vector<ColumnIndex> &column_ids,
                           const vector<string> &column_names, const vector<LogicalType> &column_types) {
	if (aggregate_p.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return string();
	}
	auto &aggregate = aggregate_p.Cast<BoundAggregateExpression>();
	if (aggregate.GetFilter() || aggregate.GetOrderBys()) {
		return string();
	}
	auto &name = aggregate.Function().GetName().GetIdentifierName();
	if (name == "count_star") {
		return aggregate.GetChildren().empty() ? "count(*)" : string();
	}
	static const char *allowed[] = {"count", "sum", "min", "max", "avg"};
	bool found = false;
	for (auto &entry : allowed) {
		if (StringUtil::CIEquals(entry, name)) {
			found = true;
			break;
		}
	}
	if (!found || aggregate.GetChildren().empty()) {
		return string();
	}
	vector<string> arguments;
	for (auto &child : aggregate.GetChildren()) {
		auto rendered = RenderComplexFilter(*child, column_ids, column_names, column_types);
		if (rendered.empty()) {
			return string();
		}
		arguments.push_back(std::move(rendered));
	}
	return StringUtil::Format("%s(%s%s)", SQLIdentifier(name), aggregate.IsDistinct() ? "DISTINCT " : "",
	                          StringUtil::Join(arguments, ", "));
}

string BuildFilterWhereClause(const TableFilterSet &filters, const vector<ColumnIndex> &column_indexes,
                              const vector<string> &column_names, const vector<LogicalType> &column_types) {
	vector<string> clauses;
	for (auto &entry : filters) {
		auto proj_idx = entry.GetIndex().GetIndex();
		if (proj_idx >= column_indexes.size()) {
			throw InternalException("quack filter pushdown: filter index %llu out of range", proj_idx);
		}
		auto &col_index = column_indexes[proj_idx];
		auto &filter = entry.Filter();
		if (col_index.IsVirtualColumn() || col_index.GetPrimaryIndex() >= column_names.size()) {
			if (filter.filter_type == TableFilterType::EXPRESSION_FILTER &&
			    ExpressionFilter::IsOptionalFilter(filter)) {
				continue;
			}
			throw InternalException("quack filter pushdown: required filter on unmappable column");
		}
		auto col_id = col_index.GetPrimaryIndex();
		BoundReferenceExpression column(Identifier(SQLIdentifier::ToString(column_names[col_id])), column_types[col_id],
		                                0);
		RenderFilterExpression(filter.ToExpression(column), clauses);
	}
	return StringUtil::Join(clauses, " AND ");
}

} // namespace duckdb
