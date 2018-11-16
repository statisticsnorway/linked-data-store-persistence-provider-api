package no.ssb.lds.api.persistence;

import java.util.concurrent.CompletableFuture;

public interface Transaction extends AutoCloseable {

    CompletableFuture<TransactionStatistics> commit();

    CompletableFuture<TransactionStatistics> cancel();

    @Override
    default void close() {
        commit().join();
    }
}
