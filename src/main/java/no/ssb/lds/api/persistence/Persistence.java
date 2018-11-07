package no.ssb.lds.api.persistence;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;

public interface Persistence {

    /**
     * @param document
     * @return timestamp of previous version of document or null if the provided document is the first version.
     * @throws PersistenceException
     */
    CompletableFuture<PersistenceResult> createOrOverwrite(Document document) throws PersistenceException;

    /**
     * Read the given document identifiers at a given point in time.
     *
     * @param timestamp a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param namespace
     * @param entity
     * @param id
     * @return the document representet by the given resource parameters and timestamp or null if not exists.
     */
    CompletableFuture<PersistenceResult> read(ZonedDateTime timestamp, String namespace, String entity, String id) throws PersistenceException;

    /**
     * @param from
     * @param to
     * @param namespace
     * @param entity
     * @param id
     * @return
     * @throws PersistenceException
     */
    CompletableFuture<PersistenceResult> readVersions(ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) throws PersistenceException;

    /**
     * @param namespace
     * @param entity
     * @param id
     * @return
     * @throws PersistenceException
     */
    CompletableFuture<PersistenceResult> readAllVersions(String namespace, String entity, String id, int limit) throws PersistenceException;

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @throws PersistenceException
     */
    CompletableFuture<PersistenceResult> delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

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
    CompletableFuture<PersistenceResult> markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    CompletableFuture<PersistenceResult> findAll(ZonedDateTime timestamp, String namespace, String entity, int limit) throws PersistenceException;

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    CompletableFuture<PersistenceResult> find(ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) throws PersistenceException;

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
