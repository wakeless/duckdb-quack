#include "storage/quack_transaction.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/quack_catalog.hpp"

namespace duckdb {

QuackTransaction::QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p,
                                   ClientContext &context_p)
    : Transaction(manager_p, context_p), quack_catalog(quack_catalog_p),
      transaction_state(QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
}

QuackTransaction::~QuackTransaction() {
}

void QuackTransaction::Start() {
	transaction_state = QuackTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void QuackTransaction::ForceStart() {
	if (transaction_state != QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
		return;
	}
	// mark started before issuing the BEGIN so the Query() below does not recurse back here
	transaction_state = QuackTransactionState::TRANSACTION_STARTED;
	try {
		// The BEGIN is the transaction's first request, so nothing depends on the session yet and it
		// may re-handshake one the server has forgotten. Later statements must not: they would run
		// outside the transaction the server lost, applying part of it while the COMMIT still fails.
		QueryInternal("BEGIN TRANSACTION", /*allow_reconnect=*/true);
	} catch (...) {
		// No remote transaction was opened - the server may be unreachable, or the catalog may
		// have failed to load on first use. Roll the state back so the COMMIT/ROLLBACK that
		// follows does not try to close a transaction that never existed and turn a plain
		// error into a fatal one.
		transaction_state = QuackTransactionState::TRANSACTION_NOT_YET_STARTED;
		throw;
	}
}

void QuackTransaction::Commit() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		Query("COMMIT");
	}
}

void QuackTransaction::Rollback() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		try {
			Query("ROLLBACK");
		} catch (const std::exception &e) {
			// The abort has effectively already happened - a session the server dropped took its
			// transaction with it - and DuckDB escalates a throwing Rollback into a fatal error that
			// poisons the whole connection. A COMMIT still fails loudly, as it must.
			auto context_ref = context.lock();
			if (context_ref) {
				DUCKDB_LOG_WARNING(*context_ref,
				                   StringUtil::Format("Quack: remote ROLLBACK failed and was ignored: %s", e.what()));
			}
		}
	}
}

QuackTransaction &QuackTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<QuackTransaction>();
}

QuackTransaction &QuackTransaction::Get(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<QuackTransaction>();
}

unique_ptr<ColumnDataCollection> QuackTransaction::Query(const string &query) {
	ForceStart();
	return QueryInternal(query, /*allow_reconnect=*/false);
}

unique_ptr<ColumnDataCollection> QuackTransaction::QueryInternal(const string &query, bool allow_reconnect) {
	auto context_ref = context.lock();
	if (!context_ref) {
		// context has been destroyed - silently ignore the query
		return nullptr;
	}
	return quack_catalog.ExecuteCommandInternal(*context_ref, query, allow_reconnect);
}

} // namespace duckdb
