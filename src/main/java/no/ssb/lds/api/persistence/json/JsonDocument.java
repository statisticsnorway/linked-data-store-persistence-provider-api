package no.ssb.lds.api.persistence.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.lds.api.persistence.DocumentKey;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class JsonDocument {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final DocumentKey key;

    private JsonNode jackson;

    @Deprecated(forRemoval = true)
    private JSONObject document;

    public JsonDocument(DocumentKey key, JsonNode document) {
        this.key = key;
        this.jackson = document;
        this.document = new JSONObject(jackson.toString());
    }

    @Deprecated(forRemoval = true)
    public JsonDocument(DocumentKey key, JSONObject document) {
        this.key = key;
        this.document = document;
        try {
            this.jackson = objectMapper.readTree(document.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DocumentKey key() {
        return key;
    }

    public JSONObject document() {
        return document;
    }

    public JsonNode jackson() {
        return jackson;
    }

    public boolean deleted() {
        return document == null;
    }

    public void traverseField(String[] schemaPath, BiConsumer<JsonNode, String> visitCallback) {
        LinkedList<String> path = new LinkedList<>();
        path.add(schemaPath[0]);
        doTraverseField(jackson, schemaPath, 1, path, visitCallback);
    }

    public void doTraverseField(JsonNode node, String[] schemaPath, int index, Deque<String> path, BiConsumer<JsonNode, String> visitCallback) {
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
