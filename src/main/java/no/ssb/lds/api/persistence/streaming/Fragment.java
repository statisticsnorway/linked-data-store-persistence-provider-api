package no.ssb.lds.api.persistence.streaming;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Fragment implements Comparable<Fragment> {

    public static final short LIMITED_CODE = 41;
    public static final short NOT_LIMITED_CODE = 42;

    public static final Fragment DONE_NOT_LIMITED = new Fragment(true, NOT_LIMITED_CODE, null, null, null, null, null, null, 0, null);

    public final static Pattern arrayIndexPattern = Pattern.compile("\\[([0-9]*)\\]");
    public final static Pattern noIndexPattern = Pattern.compile("\\[\\]");

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

    public static String computePathFromIndexUnawarePathAndIndices(String indexUnawarePath, List<Integer> indices) {
        StringBuilder sb = new StringBuilder();
        Matcher m = noIndexPattern.matcher(indexUnawarePath);
        int i = 0;
        int prevEnd = 0;
        while (m.find()) {
            sb.append(indexUnawarePath, prevEnd, m.start());
            sb.append("[");
            sb.append(indices.get(i++));
            sb.append("]");
            prevEnd = m.end();
        }
        sb.append(indexUnawarePath, prevEnd, indexUnawarePath.length());
        return sb.toString();
    }

    public static final int TRUNCATED_VALUE_LENGTH = 100;

    public static byte[] truncate(byte[] value) {
        // TODO use a hashing function (e.g. md5) instead of truncating value
        // TODO this will also provide predictable and small index key-sizes.

        if (value.length <= TRUNCATED_VALUE_LENGTH) {
            return value;
        }

        byte[] truncatedValue = new byte[TRUNCATED_VALUE_LENGTH];
        System.arraycopy(value, 0, truncatedValue, 0, truncatedValue.length);
        return truncatedValue;
    }

    final boolean streamingControl;
    final short controlCode;

    final String namespace;
    final String entity;
    final String id;
    final ZonedDateTime timestamp;
    final String path;
    final FragmentType fragmentType;
    final long offset;
    final byte[] value;

    public Fragment(String namespace, String entity, String id, ZonedDateTime timestamp, String path, FragmentType fragmentType, final long offset, byte[] value) {
        this.streamingControl = false;
        this.controlCode = 0;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
        this.path = path;
        this.fragmentType = fragmentType;
        this.offset = offset;
        this.value = value;
    }

    public Fragment(boolean streamingControl, short controlCode, String namespace, String entity, String id, ZonedDateTime timestamp, String path, FragmentType fragmentType, final long offset, byte[] value) {
        this.streamingControl = streamingControl;
        this.controlCode = controlCode;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
        this.path = path;
        this.fragmentType = fragmentType;
        this.offset = offset;
        this.value = value;
    }

    /**
     * Whether or not this is a streaming-control fragment.
     *
     * @return true iff this is a streaming control fragment.
     */
    public boolean isStreamingControl() {
        return streamingControl;
    }

    /**
     * Whether stream is done and was limited.
     *
     * @return true iff the stream was limited.
     * @throws IllegalStateException if this method is called on a fragment that is not a steaming-control fragment.
     */
    public boolean isLimited() throws IllegalStateException {
        if (!streamingControl) {
            throw new IllegalStateException("Not a steaming control fragment");
        }
        return controlCode == LIMITED_CODE;
    }

    /**
     * Whether stream is done and was not limited, i.e. all possible results were streamed.
     *
     * @return true iff the stream was not limited.
     * @throws IllegalStateException if this method is called on a fragment that is not a steaming-control fragment.
     */
    public boolean isNotLimited() throws IllegalStateException {
        if (!streamingControl) {
            throw new IllegalStateException("Not a steaming control fragment");
        }
        return controlCode == NOT_LIMITED_CODE;
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

    public FragmentType fragmentType() {
        return fragmentType;
    }

    public long offset() {
        return offset;
    }

    public byte[] value() {
        return value;
    }

    public boolean deleteMarker() {
        return FragmentType.DELETED.equals(fragmentType);
    }

    public byte[] truncatedValue() {
        return truncate(value);
    }

    @Override
    public String toString() {
        return "Fragment{" +
                "namespace='" + namespace + '\'' +
                ", entity='" + entity + '\'' +
                ", id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", path='" + path + '\'' +
                ", fragmentType=" + fragmentType +
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
        if (streamingControl && o.streamingControl) {
            return controlCode - o.controlCode;
        } else if (streamingControl) {
            return Integer.MAX_VALUE;
        } else if (o.streamingControl) {
            return Integer.MIN_VALUE;
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
        cmp = fragmentType.compareTo(o.fragmentType);
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
