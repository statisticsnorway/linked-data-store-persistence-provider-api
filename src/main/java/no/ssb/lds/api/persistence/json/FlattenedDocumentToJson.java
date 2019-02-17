package no.ssb.lds.api.persistence.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.streaming.FragmentType;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlattenedDocumentToJson {

    static final Pattern arrayNavigationPattern = Pattern.compile("([^\\[]*)\\[([0-9]+)\\]");

    final FlattenedDocument document;

    public FlattenedDocumentToJson(FlattenedDocument document) {
        this.document = document;
    }

    public JsonNode toJsonNode() {
        ObjectNode root = JsonDocument.mapper.createObjectNode();
        for (Map.Entry<String, FlattenedDocumentLeafNode> entry : document.leafNodesByPath().entrySet()) {
            String path = entry.getKey();
            FlattenedDocumentLeafNode leafNode = entry.getValue();
            String[] pathElements = path.split("\\.");
            ObjectNode parentOfLeaf = root;
            for (int i = 1; i < pathElements.length - 1; i++) {
                String pathElement = pathElements[i];
                Matcher m = arrayNavigationPattern.matcher(pathElement);
                if (m.matches()) {
                    // array-navigation
                    String arrayIdentifier = m.group(1);
                    int arrayIndex = Integer.parseInt(m.group(2));
                    if (!parentOfLeaf.has(arrayIdentifier)) {
                        parentOfLeaf.putArray(arrayIdentifier);
                    }
                    ArrayNode array = (ArrayNode) parentOfLeaf.get(arrayIdentifier);
                    if (arrayIndex >= array.size()) {
                        array.insertObject(arrayIndex);
                    }
                    parentOfLeaf = (ObjectNode) array.get(arrayIndex);
                } else {
                    // map
                    if (!parentOfLeaf.has(pathElement)) {
                        parentOfLeaf.putObject(pathElement);
                    }
                    parentOfLeaf = (ObjectNode) parentOfLeaf.get(pathElement);
                }
            }

            String leafPathElement = pathElements[pathElements.length - 1];
            Matcher m = arrayNavigationPattern.matcher(leafPathElement);
            if (m.matches()) {
                // array-navigation
                String arrayIdentifier = m.group(1);
                int arrayIndex = Integer.parseInt(m.group(2));
                if (!parentOfLeaf.has(arrayIdentifier)) {
                    parentOfLeaf.putArray(arrayIdentifier);
                }
                ArrayNode leaf = (ArrayNode) parentOfLeaf.get(arrayIdentifier);

                if (FragmentType.NUMERIC == leafNode.type()) {
                    String strValue = (String) leafNode.value();
                    // TODO Use pattern matching to find correct type rather than using exceptions
                    // TODO to control type and flow.
                    try {
                        leaf.insert(arrayIndex, Integer.valueOf(strValue));
                    } catch (NumberFormatException e) {
                        try {
                            leaf.insert(arrayIndex, Long.valueOf(strValue));
                        } catch (NumberFormatException e1) {
                            try {
                                leaf.insert(arrayIndex, Double.valueOf(strValue));
                            } catch (NumberFormatException e2) {
                                throw e2;
                            }
                        }
                    }
                } else if (FragmentType.STRING == leafNode.type()) {
                    leaf.insert(arrayIndex, (String) leafNode.value());
                } else if (FragmentType.BOOLEAN == leafNode.type()) {
                    leaf.insert(arrayIndex, Boolean.valueOf((String) leafNode.value()));
                } else if (FragmentType.EMPTY_ARRAY == leafNode.type()) {
                    leaf.insertArray(arrayIndex);
                } else if (FragmentType.EMPTY_OBJECT == leafNode.type()) {
                    leaf.insertObject(arrayIndex);
                } else if (FragmentType.NULL == leafNode.type()) {
                    leaf.insertNull(arrayIndex);
                } else if (FragmentType.DELETED == leafNode.type()) {
                    leaf.insertNull(arrayIndex);
                } else {
                    throw new UnsupportedOperationException("Unsupported FragmentType: " + leafNode.type());
                }
            } else {
                // map
                if (FragmentType.NUMERIC == leafNode.type()) {
                    String strValue = (String) leafNode.value();
                    // TODO Use pattern matching to find correct type rather than using exceptions
                    // TODO to control type and flow.
                    try {
                        parentOfLeaf.put(leafPathElement, Integer.valueOf(strValue));
                    } catch (NumberFormatException e) {
                        try {
                            parentOfLeaf.put(leafPathElement, Long.valueOf(strValue));
                        } catch (NumberFormatException e1) {
                            try {
                                parentOfLeaf.put(leafPathElement, Double.valueOf(strValue));
                            } catch (NumberFormatException e2) {
                                throw e2;
                            }
                        }
                    }
                } else if (FragmentType.STRING == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, (String) leafNode.value());
                } else if (FragmentType.BOOLEAN == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, Boolean.parseBoolean((String) leafNode.value()));
                } else if (FragmentType.EMPTY_ARRAY == leafNode.type()) {
                    parentOfLeaf.putArray(leafPathElement);
                } else if (FragmentType.EMPTY_OBJECT == leafNode.type()) {
                    parentOfLeaf.putObject(leafPathElement);
                } else if (FragmentType.DELETED == leafNode.type()) {
                    parentOfLeaf.putNull(leafPathElement);
                } else if (FragmentType.NULL == leafNode.type()) {
                    parentOfLeaf.putNull(leafPathElement);
                } else {
                    throw new UnsupportedOperationException("Unsupported FragmentType: " + leafNode.type());
                }
            }
        }
        return root;
    }
}
