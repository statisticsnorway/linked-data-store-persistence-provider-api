package no.ssb.lds.api.specification;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SpecificationElement {

    String getName();

    SpecificationElement getParent();

    SpecificationElementType getSpecificationElementType();

    Set<String> getJsonTypes();

    List<SpecificationValidator> getValidators();

    Set<String> getRefTypes();

    Map<String, SpecificationElement> getProperties();

    SpecificationElement getItems();
}
