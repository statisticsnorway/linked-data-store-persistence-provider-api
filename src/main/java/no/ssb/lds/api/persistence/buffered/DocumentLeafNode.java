package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.FragmentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class DocumentLeafNode {

    static final byte[] EMPTY = new byte[0];
    static final byte[] TRUE = new byte[]{(byte) 1};
    static final byte[] FALSE = new byte[]{(byte) 0};

    final DocumentKey key;
    final String path;
    final FragmentType type;
    final Object value;
    final int capacity;

    public DocumentLeafNode(DocumentKey key, String path, FragmentType type, Object value, int capacity) {
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

    Iterator<Fragment> fragmentIterator() {
        List<Fragment> fragments = new LinkedList<>();
        if (type == FragmentType.NULL) {
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.NULL, 0, EMPTY));
        } else if (type == FragmentType.BOOLEAN) {
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.BOOLEAN, 0, (Boolean) value ? TRUE : FALSE));
        } else if (type == FragmentType.NUMERIC) {
            byte[] fragmentValue = ((String) value).getBytes(StandardCharsets.UTF_8);
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.NUMERIC, 0, fragmentValue));
        } else if (type == FragmentType.STRING) {
            ByteBuffer out = ByteBuffer.allocate(capacity);
            CharBuffer in = CharBuffer.wrap((String) value);
            CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
            CoderResult coderResult;
            coderResult = encoder.encode(in, out, false);
            handleError(coderResult);
            int offset = 0;
            while (coderResult.isOverflow()) {
                byte[] fragmentValue = new byte[out.position()];
                System.arraycopy(out.array(), 0, fragmentValue, 0, out.position());
                fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.STRING, offset, fragmentValue));
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
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.STRING, offset, fragmentValue));
        } else if (type == FragmentType.EMPTY_OBJECT) {
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.EMPTY_OBJECT, 0, EMPTY));
        } else if (type == FragmentType.EMPTY_ARRAY) {
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, FragmentType.EMPTY_ARRAY, 0, EMPTY));
        } else {
            throw new IllegalStateException("Unknown FragmentType: " + type);
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
        return "DocumentLeafNode{" +
                "path='" + path + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocumentLeafNode that = (DocumentLeafNode) o;
        return Objects.equals(path, that.path) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, value);
    }
}
