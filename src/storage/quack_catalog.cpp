#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"
#include "quack_scan.hpp"
#include "storage/quack_insert.hpp"
#include "quack_message.hpp"
#include "quack_client.hpp"
#include "storage/quack_transaction.hpp"

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

namespace duckdb {

QuackCatalog::QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context,
                           const string &token_p, string client_id_p, bool eager_catalog)
    : Catalog(db_p), server_uri(server_uri_p), token(token_p), client_id(std::move(client_id_p)) {
	if (eager_catalog) {
		// the caller wants ATTACH to prove the server is reachable and the token is good
		EnsureLoaded(context);
	}
}

void QuackCatalog::EnsureLoaded(ClientContext &context) {
	std::unique_lock<mutex> guard(load_lock);
	// another thread may already be loading - wait for it rather than loading twice
	load_cv.wait(guard, [&] { return !loading; });
	if (loaded) {
		return;
	}
	loading = true;
	guard.unlock();

	// Deliberately outside the lock: the load itself queries the server, and when a server and
	// a client attached to it share a process that query comes back through this catalog. See
	// ScanSchemas.
	try {
		auto connection = QuackClient::ConnectToServer(context, server_uri, token, client_id);
		auto load_info = LoadCatalogWith(context, *connection);
		auto loaded_schemas = make_uniq<QuackSchemaSet>(context, *this, load_info);

		guard.lock();
		client_connection = std::move(connection);
		schemas = std::move(loaded_schemas);
		loaded = true;
	} catch (...) {
		guard.lock();
		loading = false;
		load_cv.notify_all();
		throw;
	}
	loading = false;
	load_cv.notify_all();
}

QuackLoadCatalogData QuackCatalog::LoadCatalogWith(ClientContext &context, QuackClientConnection &connection) {
	QuackLoadCatalogData result;
	result.schemas = ExecuteCommandOn(context, connection, QuackSchemaSet::GetLoadQuery());
	Value views_only_val;
	auto views_only = context.TryGetCurrentSetting("quack_catalog_views_only", views_only_val) &&
	                  !views_only_val.IsNull() && BooleanValue::Get(views_only_val);
	result.tables = ExecuteCommandOn(context, connection, QuackTableSet::GetLoadQuery(views_only));
	return result;
}

QuackCatalog::~QuackCatalog() {
}

void QuackCatalog::Initialize(bool load_builtin) {
}

optional_ptr<SchemaCatalogEntry> QuackCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto context = transaction.context;
	if (!context) {
		// no context to connect with; an unloaded catalog simply has no entries to offer
		if (!loaded) {
			return nullptr;
		}
	} else {
		EnsureLoaded(*context);
	}
	auto &schema_name = schema_lookup.GetEntryName();
	auto schema_entry = schemas ? schemas->GetEntry(schema_name) : nullptr;
	if (schema_entry) {
		return schema_entry->Cast<SchemaCatalogEntry>();
	}
	switch (if_not_found) {
	case OnEntryNotFound::THROW_EXCEPTION:
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	case OnEntryNotFound::RETURN_NULL:
	default:
		return nullptr;
	}
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommandInternal(ClientContext &context, const string &query) {
	EnsureLoaded(context);
	return ExecuteCommandOn(context, *client_connection, query);
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommandOn(ClientContext &context,
                                                                QuackClientConnection &connection,
                                                                const string &query) {
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	// get a client to query
	// A catalog load starts a session's work, so it can re-handshake if the server forgot us.
	auto response = connection.RequestWithReconnect<PrepareResponseMessage>(
	    context, [&](const string &connection_id) {
		    return make_uniq<PrepareRequestMessage>(connection_id, query, 0);
	    });
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableResults()) {
		chunk_collection->Append(chunk->Chunk());
	}
	// Drain the rest. A catalog wider than one FETCH batch used to be silently truncated,
	// dropping tables and views: harmless while the load ran during ATTACH with default
	// settings, but it loads on first use now, under whatever batch size is in effect then.
	auto query_uuid = response->QueryUUID();
	// Fetches must not re-handshake: they depend on the result the current session is holding.
	auto client_wrapper = connection.GetClient(context);
	auto &client = client_wrapper->GetClient();
	while (response->NeedsMoreFetch()) {
		auto fetch_response = client.Request<FetchResponseMessage>(
		    context, make_uniq<FetchRequestMessage>(connection.ConnectionId(), query_uuid));
		if (fetch_response->MutableResults().empty()) {
			break;
		}
		for (auto &chunk : fetch_response->MutableResults()) {
			chunk_collection->Append(chunk->Chunk());
		}
	}
	return chunk_collection;
}

shared_ptr<QuackClientConnection> QuackCatalog::GetClientConnection(ClientContext &context) {
	EnsureLoaded(context);
	return client_connection;
}

shared_ptr<QuackClientConnection> QuackCatalog::GetClientConnection() {
	if (!client_connection) {
		throw InternalException("Quack catalog was used before it was loaded");
	}
	return client_connection;
}

void QuackCatalog::Refresh(ClientContext &context) {
	{
		lock_guard<mutex> guard(load_lock);
		if (!loaded) {
			// nothing is cached yet - the next use will read a fresh catalog anyway
			return;
		}
	}
	auto load_info = LoadCatalogWith(context, *client_connection);
	schemas->Reload(context, *this, load_info);
}

const string &QuackCatalog::GetConnectionId(ClientContext &context) {
	EnsureLoaded(context);
	return client_connection->ConnectionId();
}

const string &QuackCatalog::GetConnectionId() {
	if (!client_connection) {
		throw InternalException("Quack catalog was used before it was loaded");
	}
	return client_connection->ConnectionId();
}

QuackCatalog &QuackCatalog::GetQuackCatalog(ClientContext &context, Value &catalog_name) {
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
		throw BinderException("Attached database \"%s\" does not refer to a Quack database", db_name);
	}
	return catalog.Cast<QuackCatalog>();
}

optional_ptr<CatalogEntry> QuackCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (transaction.context) {
		EnsureLoaded(*transaction.context);
	}
	auto &quack_transaction = QuackTransaction::Get(transaction);
	// create schema remotely
	quack_transaction.Query(info.ToString());
	// register schema locally
	auto schema_entry = make_uniq<QuackSchemaCatalogEntry>(*this, info);
	return schemas->CreateEntry(std::move(schema_entry), info.on_conflict);
}

void QuackCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	{
		lock_guard<mutex> guard(load_lock);
		if (loading) {
			// A load is in flight, and this scan may be that load's own catalog query coming
			// back around - a server and a client attached to it can share a process, and
			// enumerating schemas there reaches this catalog again. It has none to report
			// yet, which is the truth; waiting instead would deadlock against the load.
			return;
		}
	}
	EnsureLoaded(context);
	if (!schemas) {
		return;
	}
	for (auto &schema : schemas->GetAllCatalogEntries()) {
		callback(schema.get().Cast<SchemaCatalogEntry>());
	}
}

PhysicalOperator &QuackCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented yet");
}
PhysicalOperator &QuackCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented yet");
}

unique_ptr<LogicalOperator> QuackCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                          TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented yet");
}

DatabaseSize QuackCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented yet");
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) {
	return RemoteExecute(context, node->ToString());
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, const string &sql) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(GetName())));
	args.push_back(make_uniq<ConstantExpression>(Value(sql)));
	auto use_transaction = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
	use_transaction->SetAlias("use_transaction");
	args.push_back(std::move(use_transaction));
	auto func_ref = make_uniq<TableFunctionRef>();
	func_ref->function = make_uniq<FunctionExpression>("quack_query_by_name", std::move(args));
	return func_ref;
}

bool QuackCatalog::InMemory() {
	return false;
}
string QuackCatalog::GetDBPath() {
	// read from the stored URI, not the session: duckdb_databases() must not force a load
	return server_uri.Uri();
}

void QuackCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO should we just send over the drop info in a dropmessage???
	throw NotImplementedException("DropSchema not implemented yet");
}

bool QuackCatalog::SupportsPushdown(const TableRef &ref) {
	if (ref.type != TableReferenceType::TABLE_FUNCTION) {
		return true;
	}
	auto &table_func_ref = ref.Cast<TableFunctionRef>();
	if (table_func_ref.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return true;
	}
	auto &func_expr = table_func_ref.function->Cast<FunctionExpression>();
	if (func_expr.FunctionName() == "query") {
		return false;
	}
	return true;
}

} // namespace duckdb
