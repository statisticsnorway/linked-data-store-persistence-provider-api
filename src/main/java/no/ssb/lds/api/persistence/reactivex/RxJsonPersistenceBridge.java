package no.ssb.lds.api.persistence.reactivex;

import com.fasterxml.jackson.databind.JsonNode;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.json.FlattenedDocumentToJson;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonToFlattenedDocument;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationTraverals;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Optional.ofNullable;

/**
 *
 */
public class RxJsonPersistenceBridge implements RxJsonPersistence {

    private static final Pattern LINK_PATTERN = Pattern.compile("/(?<entity>[^/]*)/(?<id>[^/]*)");
    private final RxPersistence persistence;
    private final int fragmentSize;

    public RxJsonPersistenceBridge(RxPersistence persistence, int fragmentSize) {
        this.persistence = Objects.requireNonNull(persistence);
        this.fragmentSize = fragmentSize;
    }

    static Maybe<JsonDocument> doReadDocument(Flowable<Fragment> fragments, int fragmentSize) {
        return toDocuments(fragments, fragmentSize, false).firstElement();
    }

    /**
     * Common logic for cursor pagination.
     * <p>
     * The {@link Flowable<E>} must be ordered so that the first or last value of the {@link Range} always represent
     * the first n elements.
     * <p>
     * Strictly speaking, we could have only one implementation since [first:x, after:y] = [last:x, before:y] if the
     * stream is reversed. Maybe something to consider later.
     */
    static <E, T extends Comparable<? super T>> Flowable<E> limit(
            Flowable<E> fragments, Function<E, T> keyExtractor, Range<T> range) {
        if (range.isBackward()) {
            // Flowable MUST return values in reverse order.
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

    /**
     * Convert a flowable of {@link Fragment}s to a flowable of {@link JsonDocument}.
     * <p>
     * The received fragments must be ordered by id <strong>before</strong> it is ordered by path..
     */
    static Flowable<JsonDocument> toDocuments(Flowable<Fragment> fragmentFlowable, int fragmentSize, boolean includeDeleted) {
        return fragmentFlowable.takeWhile(fragment -> {
            // Stop when fragment is control.
            return !fragment.isStreamingControl();
        }).groupBy(fragment -> {
            // Group by id.
            return DocumentKey.from(fragment);
        }).concatMapEager(fragments -> {
            // For each group, create a FlattenedDocument.
            DocumentKey key = fragments.getKey();
            // Note that we return a Single<FlattenedDocument> so we use flatMap.
            return fragments.toMultimap(Fragment::path).map(map -> {
                return FlattenedDocument.decodeDocument(key, map, fragmentSize);
            }).toFlowable();
        }).filter(flattenedDocument -> {
            // Filter out the deleted documents.
            // TODO: Make conditional to save the rain forest.
            return includeDeleted || !flattenedDocument.deleted();
        }).map(flattenedDocument -> {
            // Convert to JsonDocument.
            return new JsonDocument(
                    flattenedDocument.key(),
                    new FlattenedDocumentToJson(flattenedDocument).toJsonNode()
            );
        });
    }

    static Flowable<JsonDocument> doReadDocuments(Flowable<Fragment> fragments, Range<String> range, int fragmentSize) {
        Flowable<JsonDocument> documents = toDocuments(fragments, fragmentSize, false);
        return limit(documents, d -> d.key().id(), range);
    }

    static Flowable<JsonDocument> doReadDocumentVersions(Flowable<Fragment> fragments, Range<ZonedDateTime> range, int fragmentSize) {
        Flowable<JsonDocument> documents = toDocuments(fragments, fragmentSize, true);
        // TODO: ZonedDateTime is a bad choice for API. Internal temporal values should be Instant.
        Range<Instant> instantRange = Range.copy(range, zonedDateTime -> zonedDateTime.toInstant());
        return limit(documents, document -> document.key().timestamp().toInstant(), instantRange);
    }

    static Flowable<JsonDocument> doFindDocuments(Flowable<Fragment> fragments, Range<String> range, int fragmentSize) {
        Flowable<JsonDocument> documents = toDocuments(fragments, fragmentSize, false);
        return limit(documents, d -> d.key().id(), range);
    }

    @Override
    public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        Flowable<Fragment> fragments = persistence.read(tx, snapshot, ns, entityName, id);
        return doReadDocument(fragments, fragmentSize);
    }

    @Override
    public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {
        Flowable<Fragment> fragments = persistence.readAll(tx, snapshot, ns, entityName, Range.unlimited(range));

        // Resort.
        fragments = range.isBackward()
                ? fragments.sorted(Comparator.reverseOrder())
                : fragments.sorted();

        return doReadDocuments(fragments, range, fragmentSize);
    }

    @Override
    public Flowable<JsonDocument> readDocumentVersions(Transaction tx, String ns, String entityName, String id,
                                                       Range<ZonedDateTime> range) {
        Flowable<Fragment> fragments = persistence.readVersions(tx, ns, entityName, id, Range.unlimited(range));

        // Resort.
        fragments = range.isBackward()
                ? fragments.sorted(Comparator.reverseOrder())
                : fragments.sorted();

        return doReadDocumentVersions(fragments, range, fragmentSize);
    }

    @Override
    public Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace,
                                               String entityName, JsonNavigationPath navigationPath, String value, Range<String> range) {
        // TODO support stronger typing of value
        Map<Integer, byte[]> valueByOffset = FlattenedDocumentLeafNode.valueByOffset(FragmentType.STRING, fragmentSize, value);
        byte[] bytesValue = valueByOffset.get(0);
        Flowable<Fragment> fragments = persistence.find(tx, snapshot, namespace, entityName, navigationPath.serialize(), bytesValue,
                Range.unlimited(range));

        // Resort.
        fragments = range.isBackward()
                ? fragments.sorted(Comparator.reverseOrder())
                : fragments.sorted();

        return doFindDocuments(fragments, range, fragmentSize).filter(document -> {
            // Post filter since fragment based implementation can return false positive.
            AtomicBoolean match = new AtomicBoolean(false);
            document.traverseField(navigationPath, (node, path) -> {
                if (node.isTextual()) {
                    boolean equals = node.textValue().equals(value);
                    if (equals) {
                        match.set(true);
                    }
                } else {
                    // TODO support matching values of other types
                }
            });
            if (match.get() == false) {
                return false; // discard false-positive match from underlying persistence layer
            }
            return true;
        });
    }

    @Override
    public Flowable<JsonDocument> readLinkedDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                      String entityName, String id, JsonNavigationPath jsonNavigationPath,
                                                      String targetEntityName, Range<String> range) {
        // TODO support reading only from relevant jsonPath in RxPersistence instead of reading entire document.
        return readDocument(tx, snapshot, ns, entityName, id)
                .flattenAsFlowable(document -> {
                    List<String> links = new ArrayList<>();
                    document.traverseField(jsonNavigationPath, (node, path) -> {
                        String link = node.asText();
                        Matcher m = LINK_PATTERN.matcher(link);
                        if (!m.matches()) {
                            return;
                        }
                        if (!targetEntityName.equals(m.group("entity"))) {
                            return;
                        }
                        links.add(m.group("id"));
                    });
                    return links;
                })
                .sorted((o1, o2) -> range.isBackward() ? o2.compareTo(o1) : o1.compareTo(o2))
                .take(ofNullable(range).map(Range::getLimit).orElse(Integer.MAX_VALUE))
                .concatMapMaybe(targetId -> readDocument(tx, snapshot, ns, targetEntityName, targetId));
    }

    @Override
    public Completable createOrOverwrite(Transaction tx, JsonDocument document, Specification specification) {
        DocumentKey key = document.key();
        JsonNode json = document.jackson();
        JsonToFlattenedDocument converter = new JsonToFlattenedDocument(key.namespace(), key.entity(), key.id(),
                key.timestamp(), json, fragmentSize);
        return persistence.createOrOverwrite(tx, Flowable.fromIterable(() -> converter.toDocument().fragmentIterator()));
    }

    @Override
    public Completable deleteDocument(Transaction tx, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        return persistence.delete(tx, ns, entityName, id, version, policy);
    }

    @Override
    public Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id, PersistenceDeletePolicy policy) {
        return persistence.deleteAllVersions(tx, ns, entity, id, policy);
    }

    @Override
    public Completable deleteAllEntities(Transaction tx, String namespace, String entity, Specification specification) {
        List<String> paths = new ArrayList<>();
        SpecificationElement entityElement = JsonNavigationPath.from("$").toSpecificationElement(specification, entity);
        SpecificationTraverals.depthFirstPreOrderFullTraversal(entityElement, (ancestors, element) -> paths.add(JsonNavigationPath.from(element).serialize()));
        return persistence.deleteAllEntities(tx, namespace, entity, paths);
    }

    @Override
    public Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        return persistence.markDeleted(transaction, ns, entityName, id, version, policy);
    }

    @Override
    public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return readDocuments(tx, snapshot, ns, entityName, Range.lastBefore(1, id)).isEmpty()
                .map(wasEmpty -> !wasEmpty);
    }

    @Override
    public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return readDocuments(tx, snapshot, ns, entityName, Range.firstAfter(1, id)).isEmpty()
                .map(wasEmpty -> !wasEmpty);
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return persistence.createTransaction(readOnly);
    }

    @Override
    public void close() throws PersistenceException {
        persistence.close();
    }
}
