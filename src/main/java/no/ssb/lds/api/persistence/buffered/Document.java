package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.FragmentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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
        return deleted == document.deleted &&
                Objects.equals(key, document.key) &&
                Objects.equals(leafNodesByPath, document.leafNodesByPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, leafNodesByPath, deleted);
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

    static Document decodeDocument(DocumentKey documentKey, Map<String, List<Fragment>> fragmentsByPath, int fragmentValueCapacityBytes) {
        TreeMap<String, DocumentLeafNode> leafNodesByPath = new TreeMap<>();
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        CharBuffer out = CharBuffer.allocate(256);
        boolean deleted = false;
        for (Map.Entry<String, List<Fragment>> entry : fragmentsByPath.entrySet()) {
            String path = entry.getKey();
            List<Fragment> fragments = entry.getValue();
            if (fragments.isEmpty()) {
                throw new IllegalStateException("No fragments for path: " + path);
            }
            FragmentType fragmentType = fragments.get(0).fragmentType();
            if (FragmentType.STRING == fragmentType) {
                StringBuilder value = new StringBuilder();
                decoder.reset();
                out.clear();
                ByteBuffer in = null;
                for (Fragment fragment : fragments) {
                    if (fragment.deleteMarker()) {
                        return new Document(documentKey, Collections.emptyMap(), true);
                    }
                    in = ByteBuffer.wrap(fragment.value());
                    CoderResult coderResult = decoder.decode(in, out, false);
                    throwRuntimeExceptionIfError(coderResult);
                    while (coderResult.isOverflow()) {
                        // drain out buffer
                        value.append(out.flip());
                        out.clear();
                        coderResult = decoder.decode(in, out, false);
                        throwRuntimeExceptionIfError(coderResult);
                    }
                    // underflow but possibly more fragments in leaf-node
                }
                // underflow and all fragments decoded
                CoderResult endOfInputCoderResult = decoder.decode(in, out, true);
                throwRuntimeExceptionIfError(endOfInputCoderResult);
                CoderResult flushCoderResult = decoder.flush(out);
                throwRuntimeExceptionIfError(flushCoderResult);
                value.append(out.flip());
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.STRING, value.toString(), fragmentValueCapacityBytes));
            } else if (FragmentType.NUMERIC == fragmentType) {
                byte[] value = fragments.get(0).value();
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.NUMERIC, new String(value, StandardCharsets.UTF_8), fragmentValueCapacityBytes));
            } else if (FragmentType.BOOLEAN == fragmentType) {
                byte[] byteValue = fragments.get(0).value();
                Boolean value = (byteValue[0] == (byte) 1) ? Boolean.TRUE : Boolean.FALSE;
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.BOOLEAN, value, fragmentValueCapacityBytes));
            } else if (FragmentType.NULL == fragmentType) {
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.NULL, null, fragmentValueCapacityBytes));
            } else if (FragmentType.EMPTY_ARRAY == fragmentType) {
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.EMPTY_ARRAY, null, fragmentValueCapacityBytes));
            } else if (FragmentType.EMPTY_OBJECT == fragmentType) {
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.EMPTY_OBJECT, null, fragmentValueCapacityBytes));
            } else if (FragmentType.DELETED == fragmentType) {
                deleted = true;
            } else {
                throw new IllegalStateException("Unknown FragmentType: " + fragmentType);
            }
        }
        return new Document(documentKey, leafNodesByPath, deleted);
    }

    static void throwRuntimeExceptionIfError(CoderResult coderResult) {
        if (coderResult.isError()) {
            try {
                coderResult.throwException();
                throw new IllegalStateException();
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
