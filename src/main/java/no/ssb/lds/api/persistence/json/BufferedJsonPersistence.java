package no.ssb.lds.api.persistence.json;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedPersistence;
import no.ssb.lds.api.specification.Specification;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;

public class BufferedJsonPersistence implements JsonPersistence {

    private final FlattenedPersistence flattenedPersistence;
    private final int fragmentValueCapacityBytes;

    public BufferedJsonPersistence(FlattenedPersistence flattenedPersistence, int fragmentValueCapacityBytes) {
        this.flattenedPersistence = flattenedPersistence;
        this.fragmentValueCapacityBytes = fragmentValueCapacityBytes;
    }

    @Override
    public TransactionFactory transactionFactory() throws PersistenceException {
        return flattenedPersistence.transactionFactory();
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return flattenedPersistence.createTransaction(readOnly);
    }

    @Override
    public CompletableFuture<Void> createOrOverwrite(Transaction transaction, JsonDocument document, Specification specification) throws PersistenceException {
        JsonToFlattenedDocument converter = new JsonToFlattenedDocument(document.key().namespace(), document.key().entity(), document.key().id(), document.key().timestamp(), document.document(), fragmentValueCapacityBytes);
        FlattenedDocument flattenedDocument = converter.toDocument();
        return flattenedPersistence.createOrOverwrite(transaction, flattenedDocument);
    }

    @Override
    public CompletableFuture<JsonDocument> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        return flattenedPersistence.read(transaction, snapshot, namespace, entity, id).thenApply(flattenedDocumentIterator -> {
            if (!flattenedDocumentIterator.hasNext()) {
                return null;
            }
            FlattenedDocument flattenedDocument = flattenedDocumentIterator.next();
            if (flattenedDocument.deleted()) {
                return new JsonDocument(flattenedDocument.key(), null);
            }
            JSONObject document = new FlattenedDocumentToJson(flattenedDocument).toJSONObject();
            return new JsonDocument(flattenedDocument.key(), document);
        });
    }

    @Override
    public CompletableFuture<Iterable<JsonDocument>> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, String firstId, int limit) throws PersistenceException {
        return flattenedPersistence.readVersions(transaction, snapshotFrom, snapshotTo, namespace, entity, id, firstId, limit)
                .thenApply(flattenedDocumentIterator -> () -> new JsonDocumentIterator(flattenedDocumentIterator));
    }

    @Override
    public CompletableFuture<Iterable<JsonDocument>> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
        return flattenedPersistence.readAllVersions(transaction, namespace, entity, id, firstVersion, limit)
                .thenApply(flattenedDocumentIterator -> () -> new JsonDocumentIterator(flattenedDocumentIterator));
    }

    @Override
    public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return flattenedPersistence.delete(transaction, namespace, entity, id, version, policy);
    }

    @Override
    public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return flattenedPersistence.deleteAllVersions(transaction, namespace, entity, id, policy);
    }

    @Override
    public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return flattenedPersistence.markDeleted(transaction, namespace, entity, id, version, policy);
    }

    @Override
    public CompletableFuture<Iterable<JsonDocument>> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
        return flattenedPersistence.findAll(transaction, snapshot, namespace, entity, firstId, limit)
                .thenApply(flattenedDocumentIterator -> () -> new JsonDocumentIterator(flattenedDocumentIterator));
    }

    @Override
    public CompletableFuture<Iterable<JsonDocument>> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, String value, String firstId, int limit) throws PersistenceException {
        return flattenedPersistence.find(transaction, snapshot, namespace, entity, path, value, firstId, limit)
                .thenApply(flattenedDocumentIterator -> () -> new JsonDocumentIterator(flattenedDocumentIterator));
    }

    @Override
    public void close() throws PersistenceException {
        flattenedPersistence.close();
    }
}
