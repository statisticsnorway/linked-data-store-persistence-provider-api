package no.ssb.lds.api.persistence.flattened;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.persistence.streaming.Persistence;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultFlattenedPersistence implements FlattenedPersistence {

    final Persistence persistence;
    final int fragmentValueCapacityBytes;

    public DefaultFlattenedPersistence(Persistence persistence, int fragmentValueCapacityBytes) {
        this.persistence = persistence;
        this.fragmentValueCapacityBytes = fragmentValueCapacityBytes;
    }

    @Override
    public TransactionFactory transactionFactory() throws PersistenceException {
        return persistence.transactionFactory();
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return persistence.createTransaction(readOnly);
    }

    @Override
    public CompletableFuture<Void> createOrOverwrite(Transaction transaction, FlattenedDocument document) throws PersistenceException {
        return persistence.createOrOverwrite(transaction, subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong();
            final Iterator<Fragment> fragmentIterator = document.fragmentIterator();
            final AtomicBoolean reEntryGuard = new AtomicBoolean(false);

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) < 0) {
                    budget.getAndIncrement(); // return stolen budget
                }

                if (!reEntryGuard.compareAndSet(false, true)) {
                    return; // re-entry protection
                }

                try {
                    while (fragmentIterator.hasNext()) {
                        if (budget.getAndDecrement() > 0) {
                            subscriber.onNext(fragmentIterator.next());
                        } else {
                            return;// budget stolen, will be retured upon next request
                        }
                    }

                    // iterator exhausted
                    subscriber.onComplete();

                } finally {
                    reEntryGuard.set(false);
                }
            }

            @Override
            public void cancel() {
                // TODO stop fragmentIterator and signal subscriber
            }
        }));
    }

    public CompletableFuture<FlattenedDocumentIterator> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.read(transaction, snapshot, namespace, entity, id);
        CompletableFuture<FlattenedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new BufferedFragmentSubscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, null, null, 1));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<FlattenedDocumentIterator> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, String firstId, int limit) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.readVersions(transaction, snapshotFrom, snapshotTo, namespace, entity, id, firstId, limit);
        CompletableFuture<FlattenedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new BufferedFragmentSubscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, null, null, limit));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<FlattenedDocumentIterator> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.readAllVersions(transaction, namespace, entity, id, firstVersion, Integer.MAX_VALUE);
        CompletableFuture<FlattenedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new BufferedFragmentSubscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, null, null, limit));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return persistence.delete(transaction, namespace, entity, id, version, policy);
    }

    public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return persistence.deleteAllVersions(transaction, namespace, entity, id, policy);
    }

    public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return persistence.markDeleted(transaction, namespace, entity, id, version, policy);
    }

    public CompletableFuture<FlattenedDocumentIterator> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.findAll(transaction, snapshot, namespace, entity, firstId, Integer.MAX_VALUE);
        CompletableFuture<FlattenedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new BufferedFragmentSubscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, null, null, limit));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<FlattenedDocumentIterator> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, String value, String firstId, int limit) throws PersistenceException {
        Map<Integer, byte[]> valueByOffset = FlattenedDocumentLeafNode.valueByOffset(FragmentType.STRING, fragmentValueCapacityBytes, value);
        byte[] bytesValue = valueByOffset.get(0);
        Flow.Publisher<Fragment> publisher = persistence.find(transaction, snapshot, namespace, entity, path, bytesValue, firstId, Integer.MAX_VALUE);
        CompletableFuture<FlattenedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new BufferedFragmentSubscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, path, value, limit));
        return iteratorCompletableFuture;
    }

    public void close() throws PersistenceException {
        persistence.close();
    }
}
