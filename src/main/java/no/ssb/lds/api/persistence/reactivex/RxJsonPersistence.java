package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;

import java.time.ZonedDateTime;

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
     * Read {@link JsonDocument}s linked to another document.
     * <p>
     * TODO: onError(PersistenceException) in case of persistence exception
     *
     * @param tx               the transaction
     * @param snapshot         upper bound of the returned version
     * @param ns               the name space
     * @param entityName       the entity name
     * @param relationName     the relation name
     * @param targetEntityName the target entity name
     * @param range            lower and upper id bounds
     */
    Flowable<JsonDocument> readLinkedDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                               String entityName, String id, String relationName, String targetEntityName, Range<String> range);

    /**
     * TODO: onError(PersistenceException) in case of persistence exception
     */
    Completable createOrOverwrite(Transaction tx, JsonDocument document, Specification specification);

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
    Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id, ZonedDateTime version,
                                    PersistenceDeletePolicy policy);

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

    Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, String path, String value, Range<String> range);

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
