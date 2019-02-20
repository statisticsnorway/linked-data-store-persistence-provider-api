package no.ssb.lds.api.persistence.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonTools {

    public static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Object> toMap(JsonNode node) {
        return mapper.convertValue(node, new TypeReference<Map<String, Object>>() {
        });
    }

    public static List<?> toList(JsonNode node) {
        return mapper.convertValue(node, new TypeReference<List>() {
        });
    }

    public static JsonNode toJsonNode(Map<? super String, Object> document) {
        return mapper.convertValue(document, JsonNode.class);
    }

    public static JsonNode toJsonNode(List<? extends Object> document) {
        return mapper.convertValue(document, JsonNode.class);
    }

    public static JsonNode toJsonNode(String json) {
        try {
            return mapper.readTree(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJson(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toPrettyJson(JsonNode node) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
