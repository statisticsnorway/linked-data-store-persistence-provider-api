package no.ssb.lds.api.persistence.streaming;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * TODO: Use Fragment in range to support partial selection.
 * TODO: Document "stream control".
 * TODO: Remove limit and emphasise on request(long n) impact.
 * TODO: Mention ordering if applicable.
 */
public interface Persistence {

    /**
     * Returns a factory that can be used to create new transactions.
     *
     * @return the transaction-factory.
     * @throws PersistenceException
     */
    TransactionFactory transactionFactory() throws PersistenceException;

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

    /**
     * Persist the stream of fragments to persistence storage within the context of the given transaction.
     *
     * @param transaction the transaction.
     * @param publisher the fragment to persist.
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> createOrOverwrite(Transaction transaction, Flow.Publisher<Fragment> publisher) throws PersistenceException;

    /**
     * Attempt to read the document identified by namespace, entity, and id from a snapshot in time of the
     * persistent storage.
     *
     * @param transaction the transaction.
     * @param snapshot    a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param id the id.
     * @return a publisher that will produce all matching fragments as a stream.
     * @throws PersistenceException
     */
    Flow.Publisher<Fragment> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException;

    /**
     * Attempt to read all distinct versions of the document identified by namespace, entity, and id given a from - to
     * range of possible snapshot in time values.
     *
     * @param transaction the transaction.
     * @param snapshotFrom from point in time.
     * @param snapshotTo to point in time.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param id the id.
     * @param firstVersion TODO: Not sure what this parameter does.
     * @param limit TODO: remove. IMO we should simply rely on the request(long n) back pressure mechanic.
     * @return a publisher that will produce all matching fragments as a stream.
     * @throws PersistenceException
     */
    Flow.Publisher<Fragment> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException;

    /**
     * TODO: What does "distinct versions" means?
     * Attempt to read all distinct versions of the document identified by namespace, entity, and id given a from - to
     * range of possible snapshot in time values.
     *
     * @param transaction the transaction.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param id the id.
     * @param firstVersion TODO: Note sure what this parameter does.
     * @param limit TODO: remove. IMO we should simply rely on the request(long n) back pressure mechanic.
     * @return a publisher that will produce all matching fragments as a stream.
     * @throws PersistenceException
     */
    Flow.Publisher<Fragment> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException;

    /**
     * Delete versioned resource specified by namespace, entity, id, and version. If the versioned resource does not
     * exist, then this method has no visible effect.
     *
     * @param transaction the transaction.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param id the id.
     * @param version a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param policy the policy used when links are present in the resource to delete.
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * Delete all existing versions of resource specified by namespace, entity, id.
     *
     * @param transaction the transaction.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param id the id.
     * @param policy the policy used when links are present in the resource to delete.
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * TODO: What is the difference btw the delete and markDeleted ?
     * Mark the given resource as deleted at the given version time.
     *
     * @param transaction the transaction.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param id the id.
     * @param version a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param policy the policy used when links are present in the resource to delete.
     * @return a completable-future that will be completed when this operation is complete.
     * @throws PersistenceException
     */
    CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * TODO: Document
     *
     * @param transaction the transaction.
     * @param namespace the namespace.
     * @param entity the entity.
     * @param firstId TODO: Remove. Use range instead.
     * @param limit TODO: remove. IMO we should simply rely on the request(long n) back pressure mechanic.
     * @return a publisher that will produce all matching fragments as a stream.
     * @throws PersistenceException
     */
    Flow.Publisher<Fragment> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException;

    /**
     *
     * TODO: Document: String firstId, int limit used as pagination.
     * TODO: Document: result are all fragments of document(s) that match Path + value.
     * @param transaction
     * @param snapshot
     * @param namespace
     * @param entity
     * @param path TODO: Represents only exact match. Maybe use interface and implement over time?
     * @param value TODO: Represents only exact match. Maybe use interface and implement over time?
     * @param firstId TODO: Remove. Use range instead.
     * @param limit TODO: remove. IMO we should simply rely on the request(long n) back pressure mechanic.
     * @return a publisher that will produce all matching fragments as a stream.
     * @throws PersistenceException
     */
    Flow.Publisher<Fragment> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, byte[] value, String firstId, int limit) throws PersistenceException;

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
