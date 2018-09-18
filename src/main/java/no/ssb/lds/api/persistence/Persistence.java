package no.ssb.lds.api.persistence;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Set;

public interface Persistence {

    /**
     *
     * @param namespace
     * @param entity
     * @param id
     * @param jsonObject
     * @param links
     * @return
     * @throws PersistenceException
     */
    boolean createOrOverwrite(String namespace, String entity, String id, JSONObject jsonObject, Set<OutgoingLink> links) throws PersistenceException;

    /**
     * Returns empty json if entity not found
     *
     * @param namespace
     * @param entity
     * @param id
     * @return
     */
    JSONObject read(String namespace, String entity, String id) throws PersistenceException;

    /**
     * @param namespace
     * @param entity
     * @param id
     * @param policy
     * @return True if entity existed, false otherwise
     * @throws PersistenceException
     */
    boolean delete(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException;

    /**
     * @param namespace
     * @param entity
     * @return
     * @throws PersistenceException
     */
    JSONArray findAll(String namespace, String entity) throws PersistenceException;

    /**
     * Clean up resources
     *
     * @throws PersistenceException
     */
    void close() throws PersistenceException;
}
