package no.ssb.lds.api.persistence;

import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;

import java.util.Map;
import java.util.Set;

public interface PersistenceInitializer {

    String persistenceProviderId();

    Set<String> configurationKeys();

    RxJsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains);
}
