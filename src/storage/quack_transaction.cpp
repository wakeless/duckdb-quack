#include "storage/quack_transaction.hpp"

#include "duckdb/common/printer.hpp"
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
	if (transaction_state == QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_STARTED;
		// BEGIN opens the session-side transaction, so nothing is lost by re-handshaking when
		// the server no longer knows the session; later statements (and COMMIT) must fail
		// instead, since the server-side transaction state is gone
		Query("BEGIN TRANSACTION", /*allow_reconnect=*/true);
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
		Query("ROLLBACK");
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

unique_ptr<ColumnDataCollection> QuackTransaction::Query(const string &query, bool allow_reconnect) {
	ForceStart();
	auto context_ref = context.lock();
	if (!context_ref) {
		// context has been destroyed - silently ignore the query
		return nullptr;
	}
	return quack_catalog.ExecuteCommandInternal(*context_ref, query, allow_reconnect);
}

} // namespace duckdb
