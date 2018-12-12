package no.ssb.lds.api.specification;

import java.util.Set;

public interface Specification {

    SpecificationElement getRootElement();

    Set<String> getManagedDomains();

    default SpecificationElement getElement(String managedDomain, String[] path) {
        SpecificationElement se = getRootElement().getProperties().get(managedDomain);
        for (int i = 0; i < path.length; i++) {
            String pathElement = path[i];
            if (se.getJsonTypes().contains("array")) {
                continue; // skip array index navigation
            }
            SpecificationElement next = se.getProperties().get(pathElement);
            se = next;
        }
        return se;
    }
}
