package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;

public interface BufferedPersistence {

    /**
     * Creates a new transaction.
     *
     * @return the newly created transaction
     * @throws PersistenceException
     */
    Transaction createTransaction() throws PersistenceException;

    /**
     * Persist the document within the context of the given transaction.
     *
     * @param transaction
     * @param document
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> createOrOverwrite(Transaction transaction, Document document) throws PersistenceException;

    /**
     * Attempt to read the document identified by namespace, entity, and id from a snapshot in time of the
     * persistent storage.
     *
     * @param transaction
     * @param snapshot    a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param namespace
     * @param entity
     * @param id
     * @return a completable future that will complete with a document-iterator when ready.
     * @throws PersistenceException
     */
    CompletableFuture<DocumentIterator> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException;

    /**
     * Attempt to read all distinct versions of the document identified by namespace, entity, and id given a from - to
     * range of possible snapshot in time values.
     *
     * @param transaction
     * @param snapshotFrom
     * @param snapshotTo
     * @param namespace
     * @param entity
     * @param id
     * @param firstId
     * @param limit
     * @return a completable future that will complete with a document-iterator when ready.
     * @throws PersistenceException
     */
    CompletableFuture<DocumentIterator> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, String firstId, int limit) throws PersistenceException;

    /**
     * Attempt to read all distinct versions of the document identified by namespace, entity, and id given a from - to
     * range of possible snapshot in time values.
     *
     * @param transaction
     * @param namespace
     * @param entity
     * @param id
     * @param firstVersion
     * @param limit
     * @return a completable future that will complete with a document-iterator when ready.
     * @throws PersistenceException
     */
    CompletableFuture<DocumentIterator> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException;

    /**
     * Delete versioned resource specified by namespace, entity, id, and version. If the versioned resource does not
     * exist, then this method has no visible effect.
     *
     * @param transaction
     * @param namespace
     * @param entity
     * @param id
     * @param version
     * @param policy
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * Delete all existing versions of resource specified by namespace, entity, id.
     *
     * @param transaction
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * Mark the given resource as deleted at the given version time.
     *
     * @param transaction
     * @param namespace
     * @param entity
     * @param id
     * @param version
     * @param policy
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * @param transaction
     * @param snapshot
     * @param namespace
     * @param entity
     * @param firstId
     * @param limit
     * @return a completable future that will complete with a document-iterator when ready.
     * @throws PersistenceException
     */
    CompletableFuture<DocumentIterator> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException;

    /**
     * @param transaction
     * @param snapshot
     * @param namespace
     * @param entity
     * @param path
     * @param value
     * @param firstId
     * @param limit
     * @return a completable future that will complete with a document-iterator when ready.
     * @throws PersistenceException
     */
    CompletableFuture<DocumentIterator> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, String value, String firstId, int limit) throws PersistenceException;

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
