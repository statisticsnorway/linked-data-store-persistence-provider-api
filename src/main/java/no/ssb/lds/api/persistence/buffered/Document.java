package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Optional.ofNullable;

public class Document {
    DocumentKey key;
    final Map<String, DocumentLeafNode> leafNodesByPath;
    final boolean deleted;

    public Document(DocumentKey key, Map<String, DocumentLeafNode> leafNodesByPath, boolean deleted) {
        this.key = key;
        this.leafNodesByPath = leafNodesByPath;
        this.deleted = deleted;
    }

    public DocumentKey key() {
        return key;
    }

    public Map<String, DocumentLeafNode> leafNodesByPath() {
        return leafNodesByPath;
    }

    public DocumentLeafNode leaf(String path) {
        return leafNodesByPath.get(path);
    }

    public boolean isDeleted() {
        return deleted;
    }

    public boolean contains(String path, String value) {
        return ofNullable(leafNodesByPath.get(path)).map(leaf -> leaf.value).map(v -> value.equals(v)).orElse(Boolean.FALSE);
    }

    @Override
    public String toString() {
        return "Document{" +
                "key=" + key +
                ", fragmentByPath=" + leafNodesByPath +
                ", deleted=" + deleted +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return Objects.equals(key, document.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public Iterator<Fragment> fragmentIterator() {
        // TODO Iterable directly rather than creating temporary collection?
        List<Fragment> allDocumentFragments = new LinkedList<>();
        for (Map.Entry<String, DocumentLeafNode> entry : leafNodesByPath.entrySet()) {
            Iterator<Fragment> fragmentIterator = entry.getValue().fragmentIterator();
            while (fragmentIterator.hasNext()) {
                Fragment fragment = fragmentIterator.next();
                allDocumentFragments.add(fragment);
            }
        }
        return allDocumentFragments.iterator();
    }
}
