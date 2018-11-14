package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.PersistenceResult;
import no.ssb.lds.api.persistence.PersistenceStatistics;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
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
            if (documentKey == null) {
                DocumentKey key = new DocumentKey(item.fragment().namespace(), item.fragment().entity(), item.fragment().id(), item.fragment().timestamp());
            }
            if (documentKey.equals(DocumentKey.from(item.fragment()))) {
                fragmentsByPath.computeIfAbsent(item.fragment().path(), path -> new ArrayList<>()).add(item.fragment());
            } else {
                documents.add(decodeDocument(documentKey, fragmentsByPath));
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            documents.add(decodeDocument(documentKey, fragmentsByPath));
            result.complete(new BufferedDocumentIterator(documents, statistics));
        }

        Document decodeDocument(DocumentKey documentKey, Map<String, List<Fragment>> fragmentsByPath) {
            TreeMap<String, DocumentLeafNode> leafNodesByPath = new TreeMap<>();
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
            CharBuffer out = CharBuffer.allocate(256);
            for (Map.Entry<String, List<Fragment>> entry : fragmentsByPath.entrySet()) {
                String path = entry.getKey();
                List<Fragment> fragments = entry.getValue();
                if (fragments.isEmpty()) {
                    throw new IllegalStateException("No fragments for path: " + path);
                }
                StringBuilder value = new StringBuilder();
                decoder.reset();
                out.clear();
                ByteBuffer in = null;
                for (Fragment fragment : fragments) {
                    if (fragment.deleteMarker()) {
                        return new Document(documentKey, Collections.emptyMap(), true);
                    }
                    in = ByteBuffer.wrap(fragment.value());
                    CoderResult coderResult = decoder.decode(in, out, false);
                    throwRuntimeExceptionIfError(coderResult);
                    while (coderResult.isOverflow()) {
                        // drain out buffer
                        value.append(out.flip());
                        out.clear();
                        coderResult = decoder.decode(in, out, false);
                        throwRuntimeExceptionIfError(coderResult);
                    }
                    // underflow but possibly more fragments in leaf-node
                }
                // underflow and all fragments decoded
                CoderResult endOfInputCoderResult = decoder.decode(in, out, true);
                throwRuntimeExceptionIfError(endOfInputCoderResult);
                CoderResult flushCoderResult = decoder.flush(out);
                throwRuntimeExceptionIfError(flushCoderResult);
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, value.toString(), fragmentValueCapacityBytes));
            }
            return new Document(documentKey, leafNodesByPath, false);
        }

        static void throwRuntimeExceptionIfError(CoderResult coderResult) {
            if (coderResult.isError()) {
                try {
                    coderResult.throwException();
                    throw new IllegalStateException();
                } catch (CharacterCodingException e) {
                    throw new RuntimeException(e);
                }
            }
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
