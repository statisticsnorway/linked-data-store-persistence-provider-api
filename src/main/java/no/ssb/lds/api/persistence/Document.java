package no.ssb.lds.api.persistence;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;

public class Document {
    final String namespace;
    final String entity;
    final String id;
    final ZonedDateTime timestamp;
    final Map<String, Fragment> fragmentByPath;
    final boolean deleted;

    public Document(String namespace, String entity, String id, ZonedDateTime timestamp, Map<String, Fragment> fragmentByPath, boolean deleted) {
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
        this.fragmentByPath = fragmentByPath;
        this.deleted = deleted;
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

    public Map<String, Fragment> getFragmentByPath() {
        return fragmentByPath;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return "Document{" +
                "namespace='" + namespace + '\'' +
                ", entity='" + entity + '\'' +
                ", id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", fragments=" + fragmentByPath +
                ", deleted=" + deleted +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return deleted == document.deleted &&
                Objects.equals(namespace, document.namespace) &&
                Objects.equals(entity, document.entity) &&
                Objects.equals(id, document.id) &&
                Objects.equals(timestamp, document.timestamp) &&
                Objects.equals(fragmentByPath, document.fragmentByPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, entity, id, timestamp, fragmentByPath, deleted);
    }
}
