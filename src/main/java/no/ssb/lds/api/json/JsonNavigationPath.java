package no.ssb.lds.api.json;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonNavigationPath {

    private static final Pattern arrayElementPattern = Pattern.compile("\\[(?<index>[0-9]*)]");

    public static JsonNavigationPath from(String serializedJsonNavigationPath) {
        return new JsonNavigationPath(deserialize(serializedJsonNavigationPath));
    }

    public static JsonNavigationPath from(String... jsonPath) {
        return new JsonNavigationPath(jsonPath);
    }

    public static JsonNavigationPath from(Collection<String> jsonPath) {
        return new JsonNavigationPath(jsonPath.toArray(new String[jsonPath.size()]));
    }

    public static JsonNavigationPath from(SpecificationElement specificationElement) {
        Deque<String> parts = new LinkedList<>();
        SpecificationElement e = specificationElement;
        while (!SpecificationElementType.ROOT.equals(e.getSpecificationElementType())
                && !SpecificationElementType.MANAGED.equals(e.getSpecificationElementType())) {
            if (e.getParent().getJsonTypes().contains("array")) {
                parts.addFirst("[]");
            } else {
                parts.addFirst(e.getName());
            }
            e = e.getParent();
        }
        parts.addFirst("$");
        return from(parts);
    }

    private final String[] path;

    JsonNavigationPath(String[] path) {
        this.path = path;
        validate();
    }

    private void validate() {
        if (path.length == 0) {
            throw new IllegalArgumentException("json-path must contain at least one element");
        }
        if (!path[0].startsWith("$")) {
            throw new IllegalArgumentException("json-path must start with '$' in its first element");
        }
        for (int i = 0; i < path.length; i++) {
            String element = path[i];
            if (arrayElementPattern.matcher(element).matches()) {
                continue;
            }
            if (element.contains("[") || element.contains("[")) {
                throw new IllegalArgumentException(String.format("json-path element at index %d contain square bracket symbol without being formatted as a proper array element"));
            }
        }
    }

    public String[] getPath() {
        return path;
    }

    private static String[] deserialize(String relationName) {
        List<String> parts = new LinkedList<>();
        String[] dotSeparated = relationName.split("\\.");
        for (String dotPart : dotSeparated) {
            if (dotPart.endsWith("]")) {
                if (dotPart.startsWith("[")) {
                    throw new IllegalArgumentException("Malformed json-path, array-index path-element must be prepended by field");
                }
                int indexOfFirstOpenSquareBracket = dotPart.indexOf("[");
                if (indexOfFirstOpenSquareBracket == -1) {
                    throw new IllegalArgumentException("Malformed json-path, path-element contains ']' without also containing '['");
                }
                String fieldIdentifier = dotPart.substring(0, indexOfFirstOpenSquareBracket);
                parts.add(fieldIdentifier); // array-field
                Matcher m = arrayElementPattern.matcher(dotPart.substring(indexOfFirstOpenSquareBracket));
                while (m.find()) {
                    parts.add(m.group()); // array-items field
                }
            } else {
                if (dotPart.contains("[") || dotPart.contains("]")) {
                    throw new IllegalArgumentException("Malformed json-path, path-element contain square-bracket symbol in a bad way");
                }
                parts.add(dotPart);
            }
        }
        return parts.toArray(new String[parts.size()]);
    }

    public String serialize() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length; i++) {
            if (arrayElementPattern.matcher(path[i]).matches()) {
                sb.append(path[i]);
            } else {
                if (i > 0) {
                    sb.append(".");
                }
                sb.append(path[i]);
            }
        }
        return sb.toString();
    }

    public SpecificationElement toSpecificationElement(Specification specification, String entity) {
        SpecificationElement se = specification.getRootElement().getProperties().get(entity);
        if (path == null) {
            return se;
        }
        for (int i = 1; i < path.length; i++) {
            String pathElement = path[i];
            if (se.getJsonTypes().contains("array")) {
                se = se.getItems();
                continue; // skip array index navigation
            }
            SpecificationElement next = se.getProperties().get(pathElement);
            if (next == null) {
                return se;
            }
            se = next;
        }
        return se;
    }

    public JsonNavigationPath popBack() {
        String[] result = new String[path.length - 1];
        for (int i = 0; i < path.length - 1; i++) {
            result[i] = path[i];
        }
        return JsonNavigationPath.from(result);
    }

    public String back() {
        return path[path.length - 1];
    }
}
