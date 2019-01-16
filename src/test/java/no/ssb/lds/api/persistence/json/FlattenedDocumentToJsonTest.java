package no.ssb.lds.api.persistence.json;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

public class FlattenedDocumentToJsonTest {

    @Test
    public void thatComplexArrayDocumentIsCorrectlyConverted() {
        DocumentKey key = new DocumentKey("ns", "E", "1", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Map<String, FlattenedDocumentLeafNode> leafNodesByPath = new LinkedHashMap<>();
        leafNodesByPath.put("$.name[0].first", new FlattenedDocumentLeafNode(key, "$.name[0].first", FragmentType.STRING, "John", 64));
        leafNodesByPath.put("$.name[0].last", new FlattenedDocumentLeafNode(key, "$.name[0].last", FragmentType.STRING, "Smith", 64));
        leafNodesByPath.put("$.name[1].first", new FlattenedDocumentLeafNode(key, "$.name[1].first", FragmentType.STRING, "Jane", 64));
        leafNodesByPath.put("$.name[1].last", new FlattenedDocumentLeafNode(key, "$.name[1].last", FragmentType.STRING, "Doe", 64));
        FlattenedDocument flattenedDocument = new FlattenedDocument(key, leafNodesByPath, false);

        JSONObject document = new FlattenedDocumentToJson(flattenedDocument).toJSONObject();

        System.out.printf("%s%n", document.toString(2));
    }
}
