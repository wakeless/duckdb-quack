#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
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

QuackCatalog::QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri, ClientContext &context,
                           const string &token)
    : Catalog(db_p) {
	// connect to the server
	client_connection = QuackClient::ConnectToServer(context, server_uri, token);

	// load the entire catalog up-front
	auto load_info = LoadCatalog(context);
	schemas = make_uniq<QuackSchemaSet>(context, *this, load_info);
}

QuackLoadCatalogData QuackCatalog::LoadCatalog(ClientContext &context) {
	QuackLoadCatalogData result;
	result.schemas = ExecuteCommandInternal(context, QuackSchemaSet::GetLoadQuery(), /*allow_reconnect=*/true);
	result.tables = ExecuteCommandInternal(context, QuackTableSet::GetLoadQuery(), /*allow_reconnect=*/true);
	return result;
}

QuackCatalog::~QuackCatalog() {
}

void QuackCatalog::Initialize(bool load_builtin) {
}

optional_ptr<SchemaCatalogEntry> QuackCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto schema_entry = schemas->GetEntry(schema_name);
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

const QuackUri &QuackCatalog::GetServerUri() {
	return client_connection->ServerURI();
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommandInternal(ClientContext &context, const string &query,
                                                                      bool allow_reconnect) {
	// FIXME this will break with many results!
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	unique_ptr<PrepareResponseMessage> response;
	if (allow_reconnect) {
		// session-initiating commands (catalog loads, a transaction-opening BEGIN) can safely
		// re-handshake when the server no longer knows this session
		auto make_prepare = [&](const string &connection_id) {
			return make_uniq<PrepareRequestMessage>(connection_id, query);
		};
		response = client_connection->RequestWithReconnect<PrepareResponseMessage>(context, make_prepare);
	} else {
		auto client_wrapper = client_connection->GetClient(context);
		auto &client = client_wrapper->GetClient();
		response =
		    client.Request<PrepareResponseMessage>(context, make_uniq<PrepareRequestMessage>(GetConnectionId(), query));
	}
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableResults()) {
		chunk_collection->Append(chunk->Chunk());
	}
	return chunk_collection;
}

shared_ptr<QuackClientConnection> QuackCatalog::GetClientConnection() {
	return client_connection;
}

void QuackCatalog::Refresh(ClientContext &context) {
	client_connection->ClearSchemaCache();
	auto load_info = LoadCatalog(context);
	schemas->Reload(context, *this, load_info);
}

string QuackCatalog::GetConnectionId() {
	return client_connection->ConnectionId();
}

optional_ptr<CatalogEntry> QuackCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &quack_transaction = QuackTransaction::Get(transaction);
	// create schema remotely
	quack_transaction.Query(info.ToString());
	// register schema locally
	auto schema_entry = make_uniq<QuackSchemaCatalogEntry>(*this, info);
	return schemas->CreateEntry(std::move(schema_entry), info.on_conflict);
}

void QuackCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
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
	return client_connection->ServerURI().Uri();
}

void QuackCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO should we just send over the drop info in a dropmessage???
	throw NotImplementedException("DropSchema not implemented yet");
}

} // namespace duckdb
