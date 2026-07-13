#include "storage/quack_view.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

QuackViewCatalogEntry::QuackViewCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateViewInfo &info_p)
    : ViewCatalogEntry(catalog_p, schema_p, info_p) {
}

string QuackViewCatalogEntry::CreateViewSQL(const string &catalog_name, const string &schema_name,
                                            const string &view_name) {
	//! This SQL will always be "FROM quack_query_by_name({catalog}, 'FROM {view_name}', eager := false);"
	//! View legs bind schema-only: pushdown routinely rewrites their query, which would
	//! discard an eagerly executed bind-time result.
	auto remote_sql = StringUtil::Format("FROM %s.%s", SQLIdentifier(schema_name), SQLIdentifier(view_name));
	return StringUtil::Format("FROM quack_query_by_name(%s, %s, eager := false)", SQLString(catalog_name),
	                          SQLString(remote_sql));
}

} // namespace duckdb
