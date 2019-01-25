package no.ssb.lds.api.persistence.reactivex;

import hu.akarnokd.rxjava2.interop.FlowInterop;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.Persistence;

import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * Temporary bridge that will help until all persistence are migrated to RxJava.
 */
public class RxPersistenceBridge implements RxPersistence {

    private final Persistence delegate;

    public RxPersistenceBridge(Persistence delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    // TODO: Maybe refactor to use Range<Fragment> instead? It's a low level API anyways.
    static Flowable<Fragment> doReadVersions(Flowable<Fragment> fragments, Range<ZonedDateTime> range) {
        return limit(fragments, Fragment::timestamp, range);
    }

    static Flowable<Fragment> doReadAll(Flowable<Fragment> fragments, Range<String> range) {
        return limit(fragments, Fragment::id, range);
    }

    /**
     * Common logic for cursor pagination.
     */
    static <E extends Comparable<? super E>, T extends Comparable<? super T>> Flowable<E> limit(
            Flowable<E> fragments, Function<E, T> keyExtractor, Range<T> range) {
        if (range.isBackward()) {
            // Read backward.
            fragments = fragments.sorted(Comparator.reverseOrder());
            if (range.hasBefore()) {
                fragments = fragments.skipWhile(fragment ->
                        keyExtractor.apply(fragment).compareTo(range.getBefore()) >= 0);
            }
            if (range.hasAfter()) {
                fragments = fragments.takeWhile(fragment ->
                        keyExtractor.apply(fragment).compareTo(range.getAfter()) > 0);
            }
            if (range.hasLast()) {
                fragments = fragments.take(range.getLast());
            }
        } else {
            // TODO: Define the contract precisely. Foundation DB does not honor order defined in Fragment.
            fragments = fragments.sorted();
            if (range.hasAfter()) {
                fragments = fragments.skipWhile(fragment ->
                        keyExtractor.apply(fragment).compareTo(range.getAfter()) <= 0);
            }
            if (range.hasBefore()) {
                fragments = fragments.takeWhile(fragment ->
                        keyExtractor.apply(fragment).compareTo(range.getBefore()) < 0);
            }
            if (range.hasFirst()) {
                fragments = fragments.take(range.getFirst());
            }
        }
        return fragments;
    }

    static Flowable<Fragment> doFind(Flowable<Fragment> fragments, Range<String> range) {
        return limit(fragments, Fragment::id, range);
    }

    static Consumer<CompletableFuture<Void>> nop() {
        return ignored -> {
            // nothing.
        };
    }

    @Override
    public TransactionFactory transactionFactory() throws PersistenceException {
        return delegate.transactionFactory();
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return delegate.createTransaction(readOnly);
    }

    @Override
    public Completable createOrOverwrite(Transaction tx, Flowable<Fragment> fragments) {
        try {
            Flow.Publisher<Fragment> fragmentPublisher = FlowInterop.toFlowPublisher(fragments);
            CompletableFuture<Void> completableFuture = delegate.createOrOverwrite(tx, fragmentPublisher);
            return Completable.fromFuture(completableFuture);
        } catch (PersistenceException pe) {
            return Completable.error(pe);
        }
    }

    @Override
    public Flowable<Fragment> read(Transaction tx, ZonedDateTime snapshot, String namespace, String entity, String id) {
        return Flowable.defer(() -> {
            Flow.Publisher<Fragment> fragmentPublisher = delegate.read(tx, snapshot, namespace, entity, id);
            return FlowInterop.fromFlowPublisher(fragmentPublisher);
        });
    }

    @Override
    public Flowable<Fragment> readVersions(Transaction tx, String namespace, String entity, String id,
                                           Range<ZonedDateTime> range) {
        return Flowable.defer(() -> {
            // Idiot implementation for now.
            Flow.Publisher<Fragment> fragmentPublisher = delegate.readAllVersions(tx, namespace, entity, id,
                    null, Integer.MAX_VALUE);
            Flowable<Fragment> fragmentFlowable = FlowInterop.fromFlowPublisher(fragmentPublisher);
            return doReadVersions(fragmentFlowable, range);
        });
    }

    @Override
    public Flowable<Fragment> readAll(Transaction tx, ZonedDateTime snapshot, String namespace, String entity,
                                      Range<String> range) {
        return Flowable.defer(() -> {
            Flow.Publisher<Fragment> fragmentPublisher = delegate.findAll(tx, snapshot, namespace, entity, null,
                    Integer.MAX_VALUE);
            Flowable<Fragment> fragmentFlowable = FlowInterop.fromFlowPublisher(fragmentPublisher);
            return doReadAll(fragmentFlowable, range);
        });
    }

    @Override
    public Flowable<Fragment> find(Transaction tx, ZonedDateTime snapshot, String namespace, String entity, String path,
                                   byte[] value, Range<String> range) {
        return Flowable.defer(() -> {
            Flow.Publisher<Fragment> fragmentPublisher = delegate.find(tx, snapshot, namespace, entity, path, value,
                    null, Integer.MAX_VALUE);
            Flowable<Fragment> fragmentFlowable = FlowInterop.fromFlowPublisher(fragmentPublisher);
            return doFind(fragmentFlowable, range);
        });
    }

    @Override
    public Completable delete(Transaction tx, String namespace, String entity, String id,
                              ZonedDateTime version, PersistenceDeletePolicy policy) {
        return Completable.defer(() -> {
            CompletableFuture<Void> future = delegate.delete(tx, namespace, entity, id, version, policy);
            return Completable.fromFuture(future);
        });
    }

    @Override
    public Completable deleteAllVersions(Transaction tx, String namespace, String entity, String id,
                                         PersistenceDeletePolicy policy) {
        return Completable.defer(() -> {
            CompletableFuture<Void> future = delegate.deleteAllVersions(tx, namespace, entity, id, policy);
            return Completable.fromFuture(future);
        });
    }

    @Override
    public Completable markDeleted(Transaction tx, String namespace, String entity, String id, ZonedDateTime version,
                                   PersistenceDeletePolicy policy) {
        return Completable.defer(() -> {
            CompletableFuture<Void> future = delegate.markDeleted(tx, namespace, entity, id, version, policy);
            return Completable.fromFuture(future);
        });
    }

    @Override
    public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, String id) {
        return readAll(tx, snapshot, namespace, entityName, Range.lastBefore(1, id)).isEmpty().map(empty -> !empty);
    }

    @Override
    public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, String id) {
        return readAll(tx, snapshot, namespace, entityName, Range.firstAfter(1, id)).isEmpty().map(empty -> !empty);
    }

    @Override
    public void close() throws PersistenceException {
        delegate.close();
    }
}
