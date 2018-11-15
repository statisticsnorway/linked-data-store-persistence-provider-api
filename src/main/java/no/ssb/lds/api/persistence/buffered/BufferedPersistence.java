package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.PersistenceResult;
import no.ssb.lds.api.persistence.PersistenceStatistics;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Buffered layer on top of Persistence streaming api that allows some additional functionality and
 * easier-to-work-with APIs.
 * <p>
 * NOTE: Because this layer buffers documents, and because documents can be of any size, this layer
 * may consume a lot of memory. When the streaming API provides the necessary functionality, it should
 * be used in favor of this layer to achieve predictable memory usage.
 */
public class BufferedPersistence {

    final Persistence persistence;
    final int fragmentValueCapacityBytes;

    public BufferedPersistence(Persistence persistence) {
        this.persistence = persistence;
        fragmentValueCapacityBytes = 8 * 1024;
    }

    /**
     * @param document
     * @return timestamp of previous version of document or null if the provided document is the first version.
     * @throws PersistenceException
     */
    public CompletableFuture<PersistenceStatistics> createOrOverwrite(Document document) throws PersistenceException {
        return persistence.createOrOverwrite(subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong();
            final AtomicReference<Iterator<Fragment>> fragmentIteratorRef = new AtomicReference<>();

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) < 0) {
                    budget.getAndIncrement(); // return stolen budget
                }
                Iterator<Fragment> fragmentIterator = fragmentIteratorRef.get();
                if (fragmentIterator == null) {
                    fragmentIteratorRef.compareAndSet(null, fragmentIterator = document.fragmentIterator());
                }
                while (fragmentIterator.hasNext()) {
                    if (budget.getAndDecrement() > 0) {
                        subscriber.onNext(fragmentIterator.next());
                    } else {
                        return;// budget stolen, will be retured upon next request
                    }
                }
                // iterator exhausted
                subscriber.onComplete();
            }

            @Override
            public void cancel() {
                // TODO stop fragmentIterator and signal subscriber
            }
        }));
    }

    /**
     * Read the given document identifiers at a given point in time.
     *
     * @param timestamp a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param namespace
     * @param entity
     * @param id
     * @return the document representet by the given resource parameters and timestamp or null if not exists.
     */
    public CompletableFuture<BufferedDocumentIterator> read(ZonedDateTime timestamp, String namespace, String entity, String id) throws PersistenceException {
        Flow.Publisher<PersistenceResult> publisher = persistence.read(timestamp, namespace, entity, id);
        CompletableFuture<BufferedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new Subscriber(iteratorCompletableFuture, fragmentValueCapacityBytes));
        return iteratorCompletableFuture;
    }

    static class Subscriber implements Flow.Subscriber<PersistenceResult> {
        final CompletableFuture<BufferedDocumentIterator> result;
        final int fragmentValueCapacityBytes;
        Flow.Subscription subscription;
        boolean limitedMatches = false;
        PersistenceStatistics statistics;
        DocumentKey documentKey;
        Map<String, List<Fragment>> fragmentsByPath = new TreeMap<>();
        final List<Document> documents = new ArrayList<>();

        Subscriber(CompletableFuture<BufferedDocumentIterator> result, int fragmentValueCapacityBytes) {
            this.result = result;
            this.fragmentValueCapacityBytes = fragmentValueCapacityBytes;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(2);
        }

        @Override
        public void onNext(PersistenceResult item) {
            if (item.limitedMatches()) {
                limitedMatches = true;
            }
            statistics = item.statistics();
            Fragment fragment = item.fragment();

            if (Fragment.DONE == fragment) {
                return;
            }

            if (documentKey == null) {
                documentKey = DocumentKey.from(fragment);
            }

            if (documentKey.equals(DocumentKey.from(fragment))) {
                fragmentsByPath.computeIfAbsent(fragment.path(), path -> new ArrayList<>()).add(fragment);
            } else {
                if (!fragmentsByPath.isEmpty()) {
                    documents.add(Document.decodeDocument(documentKey, fragmentsByPath, fragmentValueCapacityBytes));
                }
                fragmentsByPath.clear();
                fragmentsByPath.computeIfAbsent(fragment.path(), path -> new ArrayList<>()).add(fragment);
            }

            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (!fragmentsByPath.isEmpty()) {
                documents.add(Document.decodeDocument(documentKey, fragmentsByPath, fragmentValueCapacityBytes));
            }
            result.complete(new BufferedDocumentIterator(documents, statistics));
        }
    }

    /**
     * @param from
     * @param to
     * @param namespace
     * @param entity
     * @param id
     * @return
     * @throws PersistenceException
     */
    public CompletableFuture<BufferedDocumentIterator> readVersions(ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) throws PersistenceException {
        Flow.Publisher<PersistenceResult> publisher = persistence.readVersions(from, to, namespace, entity, id, limit);
        CompletableFuture<BufferedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new Subscriber(iteratorCompletableFuture, fragmentValueCapacityBytes));
        return iteratorCompletableFuture;
    }

    /**
     * @param namespace
     * @param entity
     * @param id
     * @return
     * @throws PersistenceException
     */
    public CompletableFuture<BufferedDocumentIterator> readAllVersions(String namespace, String entity, String id, int limit) throws PersistenceException {
        Flow.Publisher<PersistenceResult> publisher = persistence.readAllVersions(namespace, entity, id, limit);
        CompletableFuture<BufferedDocumentIterator> iteratorCompletableFuture = new CompletableFuture<>();
        publisher.subscribe(new Subscriber(iteratorCompletableFuture, fragmentValueCapacityBytes));
        return iteratorCompletableFuture;
    }

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @throws PersistenceException
     */
    public CompletableFuture<PersistenceStatistics> delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @throws PersistenceException
     */
    public CompletableFuture<PersistenceStatistics> deleteAllVersions(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    /**
     * Mark the given resource as deleted at the time provided by timestamp.
     *
     * @param timestamp
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @throws PersistenceException
     */
    public CompletableFuture<PersistenceStatistics> markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    public CompletableFuture<Iterator<DocumentResult>> findAll(ZonedDateTime timestamp, String namespace, String entity, int limit) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    public CompletableFuture<Iterator<DocumentResult>> find(ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) throws PersistenceException {
        throw new UnsupportedOperationException();
    }

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException {
        persistence.close();
    }
}
