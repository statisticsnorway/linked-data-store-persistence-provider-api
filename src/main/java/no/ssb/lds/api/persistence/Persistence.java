package no.ssb.lds.api.persistence;

import java.time.ZonedDateTime;
import java.util.List;

public interface Persistence {

    /**
     * @param document
     * @return timestamp of previous version of document or null if the provided document is the first version.
     * @throws PersistenceException
     */
    void createOrOverwrite(Document document) throws PersistenceException;

    /**
     * Read the given document identifiers at a given point in time.
     *
     * @param timestamp a point in time defining a virtual snapshot-time-view of the linked-data-store.
     * @param namespace
     * @param entity
     * @param id
     * @return the document representet by the given resource parameters and timestamp or null if not exists.
     */
    Document read(ZonedDateTime timestamp, String namespace, String entity, String id) throws PersistenceException;

    /**
     * @param from
     * @param to
     * @param namespace
     * @param entity
     * @param id
     * @return
     * @throws PersistenceException
     */
    List<Document> readVersions(ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) throws PersistenceException;

    /**
     * @param namespace
     * @param entity
     * @param id
     * @return
     * @throws PersistenceException
     */
    List<Document> readAllVersions(String namespace, String entity, String id, int limit) throws PersistenceException;

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @return True if entity existed, false otherwise
     * @throws PersistenceException
     */
    boolean delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * Mark the given resource as deleted at the time provided by timestamp.
     *
     * @param timestamp
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @return True if entity existed, false otherwise
     * @throws PersistenceException
     */
    boolean markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    List<Document> findAll(ZonedDateTime timestamp, String namespace, String entity, int limit) throws PersistenceException;

    /**
     * @param timestamp
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    List<Document> find(ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) throws PersistenceException;

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
