package no.ssb.lds.api.persistence.json;

import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentIterator;
import org.json.JSONObject;

import java.util.Iterator;

public class JsonDocumentIterator implements Iterator<JsonDocument> {
    private final FlattenedDocumentIterator flattenedDocumentIterator;

    public JsonDocumentIterator(FlattenedDocumentIterator flattenedDocumentIterator) {
        this.flattenedDocumentIterator = flattenedDocumentIterator;
    }

    @Override
    public boolean hasNext() {
        return flattenedDocumentIterator.hasNext();
    }

    @Override
    public JsonDocument next() {
        FlattenedDocument flattenedDocument = flattenedDocumentIterator.next();
        if (flattenedDocument.deleted()) {
            return new JsonDocument(flattenedDocument.key(), null);
        }
        JSONObject document = new FlattenedDocumentToJson(flattenedDocument).toJSONObject();
        return new JsonDocument(flattenedDocument.key(), document);
    }
}