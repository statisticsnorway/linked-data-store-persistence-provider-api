package no.ssb.lds.api.persistence;

import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;

import java.util.Map;
import java.util.Set;

public interface PersistenceInitializer {

    String persistenceProviderId();

    Set<String> configurationKeys();

    RxJsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains, Specification specification);
}
