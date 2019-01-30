package no.ssb.lds.api.persistence.reactivex;

import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;

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

    public static <T,F> Range<T> copy(Range<F> original, Function<F, T> mapper) {
        return new Range<>(
                Optional.ofNullable(original.after).map(mapper).orElse(null),
                Optional.ofNullable(original.before).map(mapper).orElse(null),
                original.first,
                original.last
        );
    }

    public static <T> Range<T> copy(Range<T> original) {
        return copy(original, t -> t);
    }

    public static <T> Range<T> unlimited(Range<T> original) {
        return new Range<>(
                original.after, original.before,
                null, null
        );
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
        String range = "[" + (hasAfter() ? after : "...") + ":" + (hasBefore() ? before : "...") + "]";
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

    /**
     * Returns <i>true</i> if the stream should be read backward.
     */
    public boolean isBackward() {
        return hasLast() || hasBefore() && !hasAfter();
    }

    /**
     * Returns <i>true</i> if either {@link #hasFirst()} or {@link #hasLast()} returns true.
     */
    public boolean isLimited() {
        return hasFirst() || hasLast();
    }

    /**
     * Returns {@link #getFirst()} or {@link #getLast()} if {@link #hasFirst()} or {@link #hasLast()} returns true,
     * respectively.
     */
    public Integer getLimit() {
        return hasFirst() ? getFirst() : hasLast() ? getLast() : null;
    }

    /**
     * Returns <i>true</i> if this range is defined with a before upper-bound.
     */
    public boolean hasBefore() {
        return before != null;
    }

    /**
     * Returns <i>true</i> if this range is defined with a after lower-bound.
     */
    public boolean hasAfter() {
        return after != null;
    }

    /**
     * Returns <i>true</i> if this range is defined with a last limit (upper-bound limit).
     */
    public boolean hasLast() {
        return last != null;
    }

    /**
     * Returns <i>true</i> if this range is defined with a first limit (lower-bound limit).
     */
    public boolean hasFirst() {
        return first != null;
    }
}
