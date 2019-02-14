package no.ssb.lds.api.specification;

import java.util.Set;

public interface Specification {

    SpecificationElement getRootElement();

    Set<String> getManagedDomains();
}
