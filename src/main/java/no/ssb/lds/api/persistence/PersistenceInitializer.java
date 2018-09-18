package no.ssb.lds.api.persistence;

import java.util.Map;
import java.util.Set;

public interface PersistenceInitializer {

    String persistenceProviderId();

    Set<String> configurationKeys();

    Persistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains);
}
