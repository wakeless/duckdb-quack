//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "storage/quack_schema.hpp"
#include "quack_uri.hpp"

#include <condition_variable>

namespace duckdb {

class QuackCatalog;
class QuackClient;
class QuackClientConnection;

class QuackCatalog : public Catalog {
public:
	explicit QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context,
	                      const string &token, string client_id = {}, bool eager_catalog = false);
	~QuackCatalog() override;

public:
	string GetCatalogType() override {
		return "quack";
	}
	static QuackCatalog &GetQuackCatalog(ClientContext &context, Value &catalog_name);
	static bool IsQuackScan(const string &name);
	bool SupportsPushdown(const TableRef &ref) override;
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override {
		switch (capability) {
		case RemoteCapability::IS_REMOTE:
		case RemoteCapability::EXECUTE_QUERY_NODE:
		case RemoteCapability::CONNECT:
			return true;
		default:
			return false;
		}
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override {
		return GetDBPath();
	}

	//! `allow_reconnect` re-handshakes and resends when the server has forgotten the session. Only
	//! pass true for a request that depends on no server-side state: a statement issued inside an
	//! open remote transaction would otherwise apply outside it.
	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query,
	                                                       bool allow_reconnect);
	const QuackUri &GetServerUri() {
		return server_uri;
	}
	//! Context-taking forms load the catalog if this is its first use; the plain forms are for
	//! call sites downstream of a resolved catalog entry, where the load has already happened.
	const string &GetConnectionId(ClientContext &context);
	const string &GetConnectionId();

	shared_ptr<QuackClientConnection> GetClientConnection(ClientContext &context);
	shared_ptr<QuackClientConnection> GetClientConnection();

	void Refresh(ClientContext &context);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	//! Read the remote catalog over `connection`, which during the initial load is not yet
	//! published as `client_connection`
	QuackLoadCatalogData LoadCatalogWith(ClientContext &context, QuackClientConnection &connection);

	//! Connect and snapshot the remote catalog, once. Every entry point that needs either the
	//! server session or a catalog entry goes through here, so an ATTACH that is never queried
	//! costs no round trips at all.
	void EnsureLoaded(ClientContext &context);

	//! Run a command against an explicit connection, bypassing EnsureLoaded - used by the load
	//! itself, which would otherwise recurse.
	unique_ptr<ColumnDataCollection> ExecuteCommandOn(ClientContext &context, QuackClientConnection &connection,
	                                                  const string &query, bool allow_reconnect);

private:
	QuackUri server_uri;
	//! Retained so the session can be established on first use rather than at ATTACH
	string token;
	string client_id;
	//! Guards `loaded`/`loading` only - never held across the network round trips of a load,
	//! so a request that arrives while a load is in flight cannot block behind it
	mutex load_lock;
	std::condition_variable load_cv;
	bool loaded = false;
	bool loading = false;
	shared_ptr<QuackClientConnection> client_connection;
	unique_ptr<QuackSchemaSet> schemas;
};

} // namespace duckdb
