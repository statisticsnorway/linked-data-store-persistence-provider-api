package no.ssb.lds.api.persistence;

import java.time.ZonedDateTime;
import java.util.List;

public class Document {
    final String namespace;
    final String entity;
    final String id;
    final ZonedDateTime timestamp;
    final List<Fragment> fragments;
    final boolean deleted;

    public Document(String namespace, String entity, String id, ZonedDateTime timestamp, List<Fragment> fragments, boolean deleted) {
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
        this.fragments = fragments;
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

    public List<Fragment> getFragments() {
        return fragments;
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
                ", fragments=" + fragments +
                ", deleted=" + deleted +
                '}';
    }
}
