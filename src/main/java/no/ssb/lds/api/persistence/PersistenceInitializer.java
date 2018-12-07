package no.ssb.lds.api.persistence;

import no.ssb.lds.api.persistence.json.JsonPersistence;

import java.util.Map;
import java.util.Set;

public interface PersistenceInitializer {

    String persistenceProviderId();

    Set<String> configurationKeys();

    JsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains);
}
