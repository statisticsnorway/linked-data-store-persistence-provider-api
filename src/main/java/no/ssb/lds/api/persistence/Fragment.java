package no.ssb.lds.api.persistence;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Fragment implements Comparable<Fragment> {

    public static final String DELETED_MARKER = "DELETED";
    public static final int TRUNCATED_VALUE_LENGTH = 100;

    public static final Fragment DONE = new Fragment(null, null, null, null, null, 0, null);

    public final static Pattern arrayIndexPattern = Pattern.compile("\\[([0-9]*)\\]");

    public static String computeIndexUnawarePath(String path, List<Integer> indices) {
        StringBuilder sb = new StringBuilder();
        Matcher m = arrayIndexPattern.matcher(path);
        int prevEnd = 0;
        while (m.find()) {
            sb.append(path, prevEnd, m.start());
            sb.append("[]");
            if (m.group(1).isEmpty()) {
                indices.add(null);
            } else {
                indices.add(Integer.valueOf(m.group(1)));
            }
            prevEnd = m.end();
        }
        sb.append(path, prevEnd, path.length());
        return sb.toString();
    }

    public static String truncate(String value) {
        if (value.length() <= TRUNCATED_VALUE_LENGTH) {
            return value;
        }
        return value.substring(0, TRUNCATED_VALUE_LENGTH);
    }

    final String namespace;
    final String entity;
    final String id;
    final ZonedDateTime timestamp;
    final String path;
    final long offset;
    final byte[] value;

    public Fragment(String namespace, String entity, String id, ZonedDateTime timestamp, String path, final long offset, byte[] value) {
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
        this.path = path;
        this.offset = offset;
        this.value = value;
    }

    public boolean samePathAs(Fragment o) {
        if (this == o) return true;
        if (o == null) return false;
        Fragment fragment = o;
        return Objects.equals(namespace, fragment.namespace) &&
                Objects.equals(entity, fragment.entity) &&
                Objects.equals(id, fragment.id) &&
                Objects.equals(timestamp, fragment.timestamp) &&
                Objects.equals(path, fragment.path);
    }

    public String namespace() {
        return namespace;
    }

    public String entity() {
        return entity;
    }

    public String id() {
        return id;
    }

    public ZonedDateTime timestamp() {
        return timestamp;
    }

    public String path() {
        return path;
    }

    public long offset() {
        return offset;
    }

    public byte[] value() {
        return value;
    }

    public boolean deleteMarker() {
        return DELETED_MARKER.equals(path);
    }

    public String truncatedValue() {
        if (value == null || value.length == 0) {
            return "";
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        CharBuffer output = CharBuffer.allocate(TRUNCATED_VALUE_LENGTH);
        ByteBuffer input = ByteBuffer.wrap(value);
        CoderResult coderResult = decoder.decode(input, output, false);
        throwRuntimeExceptionIfError(coderResult);
        if (coderResult.isUnderflow()) {
            // entire value fit in "truncated" value, all good, complete decoding sequence.
            CoderResult endOfInputCoderResult = decoder.decode(input, output, true);
            throwRuntimeExceptionIfError(endOfInputCoderResult);
            CoderResult flushCoderResult = decoder.flush(output);
            throwRuntimeExceptionIfError(flushCoderResult);
            return output.flip().toString();
        }
        if (coderResult.isOverflow()) {
            // value truncated
            return output.flip().toString();
        }
        throw new IllegalStateException();
    }

    void throwRuntimeExceptionIfError(CoderResult coderResult) {
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
        return "Fragment{" +
                "namespace='" + namespace + '\'' +
                ", entity='" + entity + '\'' +
                ", id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", path='" + path + '\'' +
                ", offset=" + offset +
                ", value=" + Arrays.toString(value) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fragment fragment = (Fragment) o;
        return offset == fragment.offset &&
                Objects.equals(namespace, fragment.namespace) &&
                Objects.equals(entity, fragment.entity) &&
                Objects.equals(id, fragment.id) &&
                Objects.equals(timestamp, fragment.timestamp) &&
                Objects.equals(path, fragment.path) &&
                Arrays.equals(value, fragment.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(namespace, entity, id, timestamp, path, offset);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public int compareTo(Fragment o) {
        if (this == o) {
            return 0;
        }
        if (this == DONE) {
            return Integer.MAX_VALUE;
        }
        int cmp;
        cmp = namespace.compareTo(o.namespace);
        if (cmp != 0) {
            return cmp;
        }
        cmp = entity.compareTo(o.entity);
        if (cmp != 0) {
            return cmp;
        }
        cmp = id.compareTo(o.id);
        if (cmp != 0) {
            return cmp;
        }
        cmp = timestamp.compareTo(o.timestamp);
        if (cmp != 0) {
            return cmp;
        }
        cmp = path.compareTo(o.path);
        if (cmp != 0) {
            return cmp;
        }
        long l = offset - o.offset;
        if (l != 0) {
            return (int) l;
        }
        return Arrays.compare(value, o.value);
    }
}
