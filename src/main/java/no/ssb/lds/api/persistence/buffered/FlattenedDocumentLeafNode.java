package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class FlattenedDocumentLeafNode {

    static final byte[] EMPTY = new byte[0];
    static final byte[] TRUE = new byte[]{(byte) 1};
    static final byte[] FALSE = new byte[]{(byte) 0};

    final DocumentKey key;
    final String path;
    final FragmentType type;
    final String value;
    final int capacity;

    public FlattenedDocumentLeafNode(DocumentKey key, String path, FragmentType type, String value, int capacity) {
        this.key = key;
        this.path = path;
        this.type = type;
        this.value = value;
        this.capacity = capacity;
    }

    public DocumentKey key() {
        return key;
    }

    public String path() {
        return path;
    }

    public FragmentType type() {
        return type;
    }

    public Object value() {
        return value;
    }

    public static Map<Integer, byte[]> valueByOffset(FragmentType type, int fragmentCapacity, String value) {
        Map<Integer, byte[]> valueByOffset = new TreeMap<>();
        if (type == FragmentType.NULL) {
            valueByOffset.put(0, EMPTY);
        } else if (type == FragmentType.BOOLEAN) {
            valueByOffset.put(0, Boolean.parseBoolean(value) ? TRUE : FALSE);
        } else if (type == FragmentType.NUMERIC) {
            valueByOffset.put(0, value.getBytes(StandardCharsets.UTF_8));
        } else if (type == FragmentType.STRING) {
            ByteBuffer out = ByteBuffer.allocate(Math.min(fragmentCapacity, 2 * value.length() + 256));
            CharBuffer in = CharBuffer.wrap(value);
            CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
            CoderResult coderResult;
            coderResult = encoder.encode(in, out, false);
            handleError(coderResult);
            int offset = 0;
            while (coderResult.isOverflow()) {
                byte[] fragmentValue = new byte[out.position()];
                System.arraycopy(out.array(), 0, fragmentValue, 0, out.position());
                valueByOffset.put(offset, fragmentValue);
                offset += out.position();
                out.clear();
                coderResult = encoder.encode(in, out, false);
                handleError(coderResult);
            }
            // underflow
            CoderResult endOfInputCoderResult = encoder.encode(in, out, true);
            handleError(endOfInputCoderResult);
            CoderResult flushCoderResult = encoder.flush(out);
            handleError(flushCoderResult);
            byte[] fragmentValue = new byte[out.position()];
            System.arraycopy(out.array(), 0, fragmentValue, 0, out.position());
            valueByOffset.put(offset, fragmentValue);
        } else if (type == FragmentType.EMPTY_OBJECT) {
            valueByOffset.put(0, EMPTY);
        } else if (type == FragmentType.EMPTY_ARRAY) {
            valueByOffset.put(0, EMPTY);
        } else {
            throw new IllegalStateException("Unknown FragmentType: " + type);
        }
        return valueByOffset;
    }

    Iterator<Fragment> fragmentIterator() {
        Map<Integer, byte[]> valueByOffset = valueByOffset(type, capacity, value);
        List<Fragment> fragments = new LinkedList<>();
        for (Map.Entry<Integer, byte[]> entry : valueByOffset.entrySet()) {
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, type, entry.getKey(), entry.getValue()));
        }
        return fragments.iterator();
    }

    static void handleError(CoderResult coderResult) {
        if (coderResult.isError()) {
            try {
                coderResult.throwException();
                throw new IllegalStateException();
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String toString() {
        return "FlattenedDocumentLeafNode{" +
                "path='" + path + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlattenedDocumentLeafNode that = (FlattenedDocumentLeafNode) o;
        return Objects.equals(path, that.path) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, value);
    }
}
