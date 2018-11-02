package no.ssb.lds.api.persistence;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Fragment {
    public final static Pattern arrayIndexPattern = Pattern.compile("\\[([0-9]*)\\]");

    final String path;
    final String value;
    final String indexUnawarePath;
    final List<Integer> indices = new LinkedList<>();

    public Fragment(String path, String value) {
        this.path = path;
        this.value = value;
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
        this.indexUnawarePath = sb.toString();
    }

    public String getPath() {
        return path;
    }

    public String getValue() {
        return value;
    }

    public String getArrayIndicesUnawarePath() {
        return indexUnawarePath;
    }

    public List<Integer> getArrayIndices() {
        return indices;
    }

    @Override
    public String toString() {
        return "Fragment{" +
                "path='" + path + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
