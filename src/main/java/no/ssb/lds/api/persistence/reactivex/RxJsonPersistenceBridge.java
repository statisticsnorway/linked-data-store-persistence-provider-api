package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.json.FlattenedDocumentToJson;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonToFlattenedDocument;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.specification.Specification;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class RxJsonPersistenceBridge implements RxJsonPersistence {

    private static final Pattern LINK_PATTERN = Pattern.compile("/(?<entity>.*)/(?<id>.*)");
    private final RxPersistence persistence;
    private final int fragmentSize;

    public RxJsonPersistenceBridge(RxPersistence persistence, int fragmentSize) {
        this.persistence = Objects.requireNonNull(persistence);
        this.fragmentSize = fragmentSize;
    }

    static Maybe<JsonDocument> doReadDocument(Flowable<Fragment> fragments, int fragmentSize) {
        return toDocuments(fragments, fragmentSize).singleElement();
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
    static Flowable<JsonDocument> toDocuments(Flowable<Fragment> fragmentFlowable, int fragmentSize) {
        return fragmentFlowable.groupBy(fragment -> {
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
            return !flattenedDocument.deleted();
        }).map(flattenedDocument -> {
            // Convert to JsonDocument.
            return new JsonDocument(
                    flattenedDocument.key(),
                    new FlattenedDocumentToJson(flattenedDocument).toJSONObject()
            );
        });
    }

    static Flowable<JsonDocument> doReadDocuments(Flowable<Fragment> fragments, Range<String> range, int fragmentSize) {
        Flowable<JsonDocument> documents = toDocuments(fragments, fragmentSize);
        return limit(documents, d -> d.key().id(), range);
    }

    @Override
    public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        Flowable<Fragment> fragments = persistence.read(tx, snapshot, ns, entityName, id);
        return doReadDocument(fragments, fragmentSize);
    }

    @Override
    public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {
        Flowable<Fragment> fragments = persistence.readAll(tx, snapshot, ns, entityName,
                Range.between(range.getAfter(), range.getBefore()));
        return doReadDocuments(fragments, range, fragmentSize);
    }

    @Override
    public Flowable<JsonDocument> readLinkedDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                      String entityName, String id, String relationName,
                                                      Range<String> range) {
        // TODO support this in RxPersistence.
        return readDocument(tx, snapshot, ns, entityName, id).flattenAsFlowable(document -> {
            return (List<String>) document.document().toMap().get(relationName);
        }).concatMapMaybe(value -> {
            Matcher matcher = LINK_PATTERN.matcher(value);
            if (matcher.matches()) {
                String otherId = matcher.group("id");
                String otherEntity = matcher.group("entity");
                return readDocument(tx, snapshot, ns, otherEntity, otherId);
            } else {
                return Maybe.empty();
            }
        });
    }

    @Override
    public Completable createOrOverwrite(Transaction tx, JsonDocument document, Specification specification) {
        DocumentKey key = document.key();
        JSONObject json = document.document();
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
}
