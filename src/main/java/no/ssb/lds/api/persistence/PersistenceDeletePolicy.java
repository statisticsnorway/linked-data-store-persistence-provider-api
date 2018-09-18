package no.ssb.lds.api.persistence;


/*
    - Always remove outgoing links
    - Cascade ALL incomings links and nodes (recursive)
    - All outgoings links must be deleted (the links are defined in the entity's json data)
    - All incoming links requires edge entity's json data references' to be deleted
 */

public enum PersistenceDeletePolicy {

    FAIL_IF_INCOMING_LINKS, // only delete if no incoming links exist, else throw error (default)
    DELETE_INCOMING_LINKS, // alter adjacent json-data
    CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES // delete entire graph depending on delete-target

}
