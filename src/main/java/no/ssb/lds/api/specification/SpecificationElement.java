package no.ssb.lds.api.specification;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface SpecificationElement {

    String getName();

    String getDescription();

    SpecificationElement getParent();

    SpecificationElementType getSpecificationElementType();

    Set<String> getJsonTypes();

    List<SpecificationValidator> getValidators();

    Set<String> getRefTypes();

    Map<String, SpecificationElement> getProperties();

    SpecificationElement getItems();

    default String jsonPath() {
        Deque<String> parts = new LinkedList<>();
        SpecificationElement e = this;
        while (!SpecificationElementType.MANAGED.equals(e.getSpecificationElementType())) {
            if (e.getParent().getJsonTypes().contains("array")) {
                parts.addFirst("[]");
            } else {
                parts.addFirst(e.getName());
            }
            e = e.getParent();
        }
        parts.addFirst("$");
        String path = parts.stream().collect(Collectors.joining(".")).replaceAll("\\.\\[]", "[]");
        return path;
    }
}
