package no.ssb.lds.api.persistence;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public interface Persistence {

    /**
     * Creates a new transaction.
     *
     * @return the newly created transaction
     * @throws PersistenceException
     */
    Transaction createTransaction() throws PersistenceException;

    /**
     * Persist the stream of fragments to persistence storage within the context of the given transaction.
     *
     * @param transaction
     * @param publisher
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> createOrOverwrite(Transaction transaction, Flow.Publisher<Fragment> publisher) throws PersistenceException;

    /**
     * Attempt to read the document identified by namespace, entity, and id from a snapshot in time of the
     * persistent storage.
     *
     * @param transaction
     * @param snapshot    a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param namespace
     * @param entity
     * @param id
     * @return a publisher that will produce all matching fragments as a stream of fragment results.
     * @throws PersistenceException
     */
    Flow.Publisher<FragmentResult> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException;

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
     * @return a publisher that will produce all matching fragments as a stream of fragment results.
     * @throws PersistenceException
     */
    Flow.Publisher<FragmentResult> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, String firstId, int limit) throws PersistenceException;

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
     * @return a publisher that will produce all matching fragments as a stream of fragment results.
     * @throws PersistenceException
     */
    Flow.Publisher<FragmentResult> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException;

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
     * @return a publisher that will produce all matching fragments as a stream of fragment results.
     * @throws PersistenceException
     */
    Flow.Publisher<FragmentResult> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException;

    /**
     * @param transaction
     * @param snapshot
     * @param namespace
     * @param entity
     * @param path
     * @param value
     * @param firstId
     * @param limit
     * @return a publisher that will produce all matching fragments as a stream of fragment results.
     * @throws PersistenceException
     */
    Flow.Publisher<FragmentResult> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, String value, String firstId, int limit) throws PersistenceException;

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
