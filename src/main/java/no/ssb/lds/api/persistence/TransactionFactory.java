package no.ssb.lds.api.persistence;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface TransactionFactory {

    <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends CompletableFuture<T>> retryable);

    /**
     * Create a new transaction.
     *
     * @param readOnly true if the transaction will only perform read operations, false if at least one write operation
     *                 will be performed, and false if the caller is unsure. Note that the underlying persistence
     *                 provider may be able to optimize performance and contention related issues when read-only
     *                 transactions are involved.
     * @return the newly created transaction
     * @throws PersistenceException
     */
    Transaction createTransaction(boolean readOnly) throws PersistenceException;

    void close();
}
