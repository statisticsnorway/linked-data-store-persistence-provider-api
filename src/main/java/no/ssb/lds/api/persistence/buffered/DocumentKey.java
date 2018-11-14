package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;

import java.time.ZonedDateTime;
import java.util.Objects;

public class DocumentKey {

    public static DocumentKey from(Fragment fragment) {
        return new DocumentKey(fragment.namespace(), fragment.entity(), fragment.id(), fragment.timestamp());
    }

    final String namespace;
    final String entity;
    final String id;
    final ZonedDateTime timestamp;

    public DocumentKey(String namespace, String entity, String id, ZonedDateTime timestamp) {
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getEntity() {
        return entity;
    }

    public String getId() {
        return id;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "DocumentKey{" +
                "namespace='" + namespace + '\'' +
                ", entity='" + entity + '\'' +
                ", id='" + id + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocumentKey that = (DocumentKey) o;
        return Objects.equals(namespace, that.namespace) &&
                Objects.equals(entity, that.entity) &&
                Objects.equals(id, that.id) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, entity, id, timestamp);
    }
}
