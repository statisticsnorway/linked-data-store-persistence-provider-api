package no.ssb.lds.api.persistence.flattened;

import java.util.Comparator;
import java.util.regex.Pattern;

public class PathComparator implements Comparator<String> {

    private static final Pattern SEPARATOR_PATTERN = Pattern.compile("(\\.|\\[|\\]\\.?)");

    boolean isDigit(CharSequence seq) {
        for (int j = 0; j < seq.length(); j++) {
            char charAt = seq.charAt(j);
            if (!('0' <= charAt && charAt <= '9')) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compare(String path1, String path2) {
        // Kinda optimized. Could be simpler but this code is called often.
        String[] parts1 = SEPARATOR_PATTERN.split(path1);
        String[] parts2 = SEPARATOR_PATTERN.split(path2);
        if (parts1.length != parts2.length) {
            return parts1.length < parts2.length ? -1 : +1;
        } else {
            int compare = 0;
            for (int i = 0; i < parts1.length; i++) {
                if (isDigit(parts1[i]) && isDigit(parts2[i])) {
                    compare = Integer.compareUnsigned(
                            Integer.parseInt(parts1[i]),
                            Integer.parseInt(parts2[i])
                    );
                } else {
                    compare = parts1[i].compareTo(parts2[i]);
                }
                if (compare != 0) {
                    return compare;
                }
            }
            return compare;
        }
    }
}
