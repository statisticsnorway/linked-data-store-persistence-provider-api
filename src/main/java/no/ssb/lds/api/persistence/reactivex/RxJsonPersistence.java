package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.batch.Batch;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Temporary interface
 */
public interface RxJsonPersistence {

    /**
     * Read a single {@link JsonDocument}.
     * <p>
     * TODO: onError(PersistenceException) in case of persistence exception
     *
     * @param tx         The transaction to use.
     * @param snapshot   Upper bound of the returned version
     * @param ns         the name space
     * @param entityName the entity name
     * @param id         the entity id
     * @return a {@link Maybe<JsonDocument>} instance
     * @throws PersistenceException in case of persistence exception.
     */
    Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id);

    /**
     * Read {@link JsonDocument}s.
     * <p>
     * TODO: onError(PersistenceException) in case of persistence exception
     *
     * @param tx         the transaction
     * @param snapshot   upper bound of the returned version
     * @param ns         the name space
     * @param entityName the entity name
     * @param range      lower and upper id bounds
     */
    Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName,
                                         Range<String> range);

    /**
     * Read {@link JsonDocument}s.
     * <p>
     * TODO: onError(PersistenceException) in case of persistence exception
     *
     * @param tx         the transaction
     * @param ns         the name space
     * @param entityName the entity name
     * @param range      lower and upper id bounds
     */
    Flowable<JsonDocument> readDocumentVersions(Transaction tx, String ns, String entityName, String id,
                                                Range<ZonedDateTime> range);


    /**
     * Read the <b>target</b> {@link JsonDocument}s linked to a <b>source</b> document.
     *
     * @param tx               the transaction
     * @param snapshot         upper bound of the returned version
     * @param ns               the name space
     * @param sourceEntityName the source entity name
     * @param sourceId         the source id
     * @param relationPath     the json-navigation-path
     * @param targetEntityName the target entity name
     * @param range            lower and upper id bounds
     * @see #readSourceDocuments(Transaction, ZonedDateTime, String, String, String, JsonNavigationPath, String, Range)
     */
    Flowable<JsonDocument> readTargetDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                               String sourceEntityName, String sourceId, JsonNavigationPath relationPath,
                                               String targetEntityName, Range<String> range);

    @Deprecated
    default Flowable<JsonDocument> readLinkedDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                       String sourceEntityName, String sourceId, JsonNavigationPath relationPath,
                                                       String targetEntityName, Range<String> range) {
        return readTargetDocuments(tx, snapshot, ns, sourceEntityName, sourceId, relationPath, targetEntityName, range);
    }

    /**
     * Read the <b>source</b> {@link JsonDocument}s that links to a <b>target</b> document.
     * <p>
     * Note that the range applies to the source documents.
     *
     * @param tx               the transaction
     * @param snapshot         upper bound of the returned version
     * @param ns               the name space
     * @param targetEntityName the target entity name
     * @param targetId         the target id
     * @param range            lower and upper id bounds
     * @see #readTargetDocuments(Transaction, ZonedDateTime, String, String, String, JsonNavigationPath, String, Range)
     */
    Flowable<JsonDocument> readSourceDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                               String targetEntityName, String targetId, JsonNavigationPath relationPath,
                                               String sourceEntityName, Range<String> range);

    /**
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    default Completable createOrOverwrite(Transaction tx, JsonDocument document, Specification specification) {
        return createOrOverwrite(tx, Flowable.just(document), specification);
    }

    default Completable putBatchGroup(Transaction tx, Batch.PutGroup group, String namespace, Specification specification) {
        List<JsonDocument> documents = new ArrayList<>();
        for (Batch.Entry entry : group.entries()) {
            String entity = group.type();
            String id = entry.id();
            ZonedDateTime timestamp = entry.timestamp();
            DocumentKey documentKey = new DocumentKey(namespace, entity, id, timestamp);
            JsonDocument jsonDocument = new JsonDocument(documentKey, entry.dataNode());
            documents.add(jsonDocument);
        }
        return createOrOverwrite(tx, Flowable.fromIterable(documents), specification);
    }

    default Flowable<String> resolveMatchInBatchGroup(Transaction tx, Batch.DeleteGroup group, String namespace, Specification specification) {
        // TODO implement batch delete match evaluation
        throw new UnsupportedOperationException("Not implemented");
    }

    default Completable deleteBatchGroup(Transaction tx, Batch.DeleteGroup group, String namespace, Specification specification) {
        if (group.entries().isEmpty()) {
            return Completable.complete();
        }
        List<Completable> completables = new ArrayList<>();
        for (Batch.Entry entry : group.entries()) {
            Completable completable = markDocumentDeleted(tx, namespace, group.type(), entry.id(), entry.timestamp(), null);
            completables.add(completable);
        }
        return Completable.merge(completables);
    }

    /**
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Completable createOrOverwrite(Transaction tx, Flowable<JsonDocument> documentFlowable, Specification specification);

    /**
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Completable deleteDocument(Transaction tx, String ns, String entityName, String id, ZonedDateTime version,
                               PersistenceDeletePolicy policy);

    /**
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id,
                                          PersistenceDeletePolicy policy);

    Completable deleteAllEntities(Transaction tx, String namespace, String entity, Specification specification);

    /**
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id,
                                    ZonedDateTime version, PersistenceDeletePolicy policy);

    /**
     * Checks if there is a {@link JsonDocument} before the given id.
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id);

    /**
     * Checks if there is a {@link JsonDocument} after the given id.
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id);

    /**
     * Uses the transaction-factory to create a new transaction. This method is equivalent
     * to calling <code>transactionFactory.createTransaction()</code>.
     *
     * @param readOnly true if the transaction will only perform read operations, false if at least one write operation
     *                 will be performed, and false if the caller is unsure. Note that the underlying persistence
     *                 provider may be able to optimize performance and contention related issues when read-only
     *                 transactions are involved.
     * @return the newly created transaction
     * @throws PersistenceException
     */
    Transaction createTransaction(boolean readOnly) throws PersistenceException;

    Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName,
                                        JsonNavigationPath path, String value, Range<String> range);

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;

    default <T> T getInstance(Class<T> clazz) {
        return null;
    }
}
