package no.ssb.lds.api.persistence.reactivex;

import java.util.StringJoiner;

public class Range<T> {

    private final T before;
    private final T after;
    private final Integer first;
    private final Integer last;

    private Range(T after, T before, Integer first, Integer last) {
        if (first != null && last != null) {
            throw new IllegalArgumentException("range cannot use both first and last limits");
        }
        this.before = before;
        this.after = after;
        this.first = first;
        this.last = last;
    }

    public static <T> Range<T> first(Integer n) {
        return new Range<>(null, null, n, null);
    }

    public static <T> Range<T> last(Integer n) {
        return new Range<>(null, null, null, n);
    }

    public static <T> Range<T> firstAfter(Integer n, T id) {
        return new Range<>(id, null, n, null);
    }

    public static <T> Range<T> lastBefore(Integer n, T id) {
        return new Range<>(null, id, null, n);
    }

    public static <T> Range<T> firstBetween(Integer n, T after, T before) {
        return new Range<>(after, before, n, null);
    }

    public static <T> Range<T> lastBetween(Integer n, T after, T before) {
        return new Range<>(after, before, null, n);
    }

    public static <T> Range<T> unbounded() {
        return new Range<>(null, null, null, null);
    }

    public static <T> Range<T> between(T after, T before) {
        return new Range<>(after, before, null, null);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", Range.class.getSimpleName() + "[", "]");
        String range = "[ " + (hasAfter() ? after : "...") + " : " + (hasBefore() ? before : "...") + " ]";
        joiner.add("range=" + range);

        if (hasFirst()) {
            joiner.add("first=" + first);
        }
        if (hasLast()) {
            joiner.add("last=" + last);
        }
        return joiner.toString();
    }

    public Integer getFirst() {
        return first;
    }

    public Integer getLast() {
        return last;
    }

    public T getBefore() {
        return before;
    }

    public T getAfter() {
        return after;
    }

    public boolean isBackward() {
        return hasLast() || hasBefore() && !hasAfter(); // || first == null && before != null;
    }

    public boolean hasBefore() {
        return before != null;
    }

    public boolean hasAfter() {
        return after != null;
    }

    public boolean hasLast() {
        return last != null;
    }

    public boolean hasFirst() {
        return first != null;
    }
}
