package no.ssb.lds.api.specification;

import graphql.schema.idl.TypeDefinitionRegistry;

import java.util.Set;

public interface Specification {

    SpecificationElement getRootElement();

    Set<String> getManagedDomains();

    default TypeDefinitionRegistry typeDefinitionRegistry() {
        throw new UnsupportedOperationException();
    }
}
