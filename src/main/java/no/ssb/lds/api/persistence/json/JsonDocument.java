package no.ssb.lds.api.persistence.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class JsonDocument {

    public static final ObjectMapper mapper = new ObjectMapper();

    private final DocumentKey key;

    private JsonNode jackson;

    public JsonDocument(DocumentKey key, Map<String, Object> document) {
        this.key = key;
        this.jackson = JsonTools.toJsonNode(document);
    }

    public JsonDocument(DocumentKey key, JsonNode document) {
        this.key = key;
        this.jackson = document;
    }

    public DocumentKey key() {
        return key;
    }

    public JsonNode jackson() {
        return jackson;
    }

    public Map<String, Object> toMap() {
        return JsonTools.toMap(jackson);
    }

    public boolean deleted() {
        return jackson == null;
    }

    public void traverseField(JsonNavigationPath jsonNavigationPath, BiConsumer<JsonNode, String> visitCallback) {
        LinkedList<String> path = new LinkedList<>();
        path.add(jsonNavigationPath.getPath()[0]);
        doTraverseField(jackson, jsonNavigationPath.getPath(), 1, path, visitCallback);
    }

    private void doTraverseField(JsonNode node, String[] schemaPath, int index, Deque<String> path, BiConsumer<JsonNode, String> visitCallback) {
        if (index >= schemaPath.length) {
            visitCallback.accept(node, path.stream().collect(Collectors.joining(".")).replaceAll("\\.\\[", "["));
            return;
        }
        String childName = schemaPath[index];

        if ("[]".equals(childName)) {
            ArrayNode arrayNode = (ArrayNode) node;
            for (int i = 0; i < arrayNode.size(); i++) {
                JsonNode child = arrayNode.get(i);
                path.addLast("[" + i + "]");
                doTraverseField(child, schemaPath, index + 1, path, visitCallback);
                path.removeLast();
            }
        } else {
            // Object node
            JsonNode childNode = node.get(childName);
            path.addLast(childName);
            doTraverseField(childNode, schemaPath, index + 1, path, visitCallback);
            path.removeLast();
        }
    }
}
