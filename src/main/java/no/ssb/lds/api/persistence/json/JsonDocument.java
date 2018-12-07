package no.ssb.lds.api.persistence.json;

import no.ssb.lds.api.persistence.DocumentKey;
import org.json.JSONObject;

public class JsonDocument {

    private final DocumentKey key;
    private JSONObject document;

    public JsonDocument(DocumentKey key, JSONObject document) {
        this.key = key;
        this.document = document;
    }

    public DocumentKey key() {
        return key;
    }

    public JSONObject document() {
        return document;
    }

    public boolean deleted() {
        return document == null;
    }
}
