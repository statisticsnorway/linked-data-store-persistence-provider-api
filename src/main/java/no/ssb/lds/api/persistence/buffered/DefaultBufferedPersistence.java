package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Buffered layer on top of Persistence streaming api that allows some additional functionality and
 * easier-to-work-with APIs.
 * <p>
 * NOTE: Because this layer buffers documents, and because documents can be of any size, this layer
 * may consume a lot of memory. When the streaming API provides the necessary functionality, it should
 * be used in favor of this layer to achieve predictable memory usage.
 */
public class DefaultBufferedPersistence implements BufferedPersistence {

    final Persistence persistence;
    final int fragmentValueCapacityBytes;

    public DefaultBufferedPersistence(Persistence persistence) {
        this.persistence = persistence;
        fragmentValueCapacityBytes = 8 * 1024;
    }

    @Override
    public Transaction createTransaction() throws PersistenceException {
        return persistence.createTransaction();
    }

    @Override
    public CompletableFuture<Void> createOrOverwrite(Transaction transaction, Document document) throws PersistenceException {
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

    static class Subscriber implements Flow.Subscriber<Fragment> {
        final CompletableFuture<DocumentIterator> result;
        final int fragmentValueCapacityBytes;
        final int limit;

        Flow.Subscription subscription;
        boolean limitedMatches = false;
        DocumentKey documentKey;
        final Map<String, List<Fragment>> fragmentsByPath = new TreeMap<>();
        final List<Document> documents = new ArrayList<>();

        Subscriber(CompletableFuture<DocumentIterator> result, int fragmentValueCapacityBytes, int limit) {
            this.result = result;
            this.fragmentValueCapacityBytes = fragmentValueCapacityBytes;
            this.limit = limit;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(2);
        }

        @Override
        public void onNext(Fragment fragment) {
            if (fragment.isStreamingControl()) {
                limitedMatches = fragment.isLimited();
                return;
            }

            if (Fragment.DONE_NOT_LIMITED == fragment) {
                return;
            }

            if (documents.size() >= limit) {
                // document limit reached
                subscription.cancel();
                return;
            }

            DocumentKey fragmentDocumentKey = DocumentKey.from(fragment);

            if (documentKey == null) {
                documentKey = fragmentDocumentKey;
            }

            if (documentKey.equals(fragmentDocumentKey)) {
                fragmentsByPath.computeIfAbsent(fragment.path(), path -> new ArrayList<>()).add(fragment);
            } else {
                addPendingDocumentAndResetMap();
                fragmentsByPath.computeIfAbsent(fragment.path(), path -> new ArrayList<>()).add(fragment);
                documentKey = fragmentDocumentKey;
            }

            subscription.request(1);
        }

        void addPendingDocumentAndResetMap() {
            if (!fragmentsByPath.isEmpty()) {
                documents.add(Document.decodeDocument(documentKey, fragmentsByPath, fragmentValueCapacityBytes));
                fragmentsByPath.clear();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            addPendingDocumentAndResetMap();
            result.complete(new DocumentIterator(documents));
        }
    }

    public CompletableFuture<DocumentIterator> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.read(transaction, snapshot, namespace, entity, id);
        CompletableFuture<DocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new Subscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, 1));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<DocumentIterator> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, String firstId, int limit) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.readVersions(transaction, snapshotFrom, snapshotTo, namespace, entity, id, firstId, limit);
        CompletableFuture<DocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new Subscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, limit));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<DocumentIterator> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
        Flow.Publisher<Fragment> publisher = persistence.readAllVersions(transaction, namespace, entity, id, firstVersion, Integer.MAX_VALUE);
        CompletableFuture<DocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new Subscriber(iteratorCompletableFuture, fragmentValueCapacityBytes, limit));
        return iteratorCompletableFuture;
    }

    public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<DocumentIterator> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<DocumentIterator> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, String value, String firstId, int limit) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    public void close() throws PersistenceException {
        persistence.close();
    }
}
