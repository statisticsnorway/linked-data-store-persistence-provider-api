package no.ssb.lds.api.persistence.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonTools {

    public static Map<String, Object> toMap(JsonNode node) {
        return JsonDocument.mapper.convertValue(node, new TypeReference<Map<String, Object>>() {
        });
    }

    public static List<?> toList(JsonNode node) {
        return JsonDocument.mapper.convertValue(node, new TypeReference<List>() {
        });
    }

    public static JsonNode toJsonNode(Map<? super String, Object> document) {
        return JsonDocument.mapper.convertValue(document, JsonNode.class);
    }

    public static JsonNode toJsonNode(List<? extends Object> document) {
        return JsonDocument.mapper.convertValue(document, JsonNode.class);
    }

    public static JsonNode toJsonNode(String json) {
        try {
            return JsonDocument.mapper.readTree(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJson(JsonNode node) {
        try {
            return JsonDocument.mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
