package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Flowable;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.time.ZonedDateTime.parse;
import static no.ssb.lds.api.persistence.reactivex.RxPersistenceBridge.doFind;
import static no.ssb.lds.api.persistence.reactivex.RxPersistenceBridge.doReadAll;
import static no.ssb.lds.api.persistence.reactivex.RxPersistenceBridge.doReadVersions;
import static org.assertj.core.api.Assertions.assertThat;

public class RxPersistenceBridgeTest {

    private static Fragment createFragmentWithSnapshot(ZonedDateTime timestamp) {
        return createFragment(timestamp, "id", "path");
    }

    private static Fragment createFragmentWithId(String id) {
        return createFragmentWithIdAndPath(id, "path");
    }

    private static Fragment createFragmentWithIdAndPath(String id, String path) {
        return createFragment(parse("2000-01-01T00:00:00.000Z"), id, path);
    }

    private static Fragment createFragment(ZonedDateTime timestamp, String id, String path) {
        return new Fragment("ns", "entity", id, timestamp, path, FragmentType.STRING, 0, null);
    }

    @Test
    public void testDoReadVersions() {
        // Test data, version each 1st of month of 2000 at 00:00:00.000Z.
        ZonedDateTime timestamp = parse("2000-01-01T00:00:00.000Z");
        SortedSet<Fragment> fragments = new TreeSet<>();
        for (int i = 1; i <= 12; i++) {
            fragments.add(createFragmentWithSnapshot(timestamp.withMonth(i)));
        }
        Flowable<Fragment> fragmentFlowable = Flowable.fromIterable(fragments);

        Flowable<Fragment> firstSixAfterMars = doReadVersions(fragmentFlowable, Range.firstAfter(6, timestamp.withMonth(3)));
        assertThat(firstSixAfterMars.blockingIterable())
                .containsExactly(
                        createFragmentWithSnapshot(timestamp.withMonth(4)),
                        createFragmentWithSnapshot(timestamp.withMonth(5)),
                        createFragmentWithSnapshot(timestamp.withMonth(6)),
                        createFragmentWithSnapshot(timestamp.withMonth(7)),
                        createFragmentWithSnapshot(timestamp.withMonth(8)),
                        createFragmentWithSnapshot(timestamp.withMonth(9))
                );

        Flowable<Fragment> lastSixBeforeOctober = doReadVersions(fragmentFlowable, Range.lastBefore(6, timestamp.withMonth(10)));
        assertThat(lastSixBeforeOctober.blockingIterable())
                .containsExactly(
                        createFragmentWithSnapshot(timestamp.withMonth(9)),
                        createFragmentWithSnapshot(timestamp.withMonth(8)),
                        createFragmentWithSnapshot(timestamp.withMonth(7)),
                        createFragmentWithSnapshot(timestamp.withMonth(6)),
                        createFragmentWithSnapshot(timestamp.withMonth(5)),
                        createFragmentWithSnapshot(timestamp.withMonth(4))
                );

        Flowable<Fragment> firstThreeBetweenAprilAndSeptember = doReadVersions(fragmentFlowable, Range.firstBetween(
                3, timestamp.withMonth(4), timestamp.withMonth(9)));
        assertThat(firstThreeBetweenAprilAndSeptember.blockingIterable())
                .containsExactly(
                        createFragmentWithSnapshot(timestamp.withMonth(5)),
                        createFragmentWithSnapshot(timestamp.withMonth(6)),
                        createFragmentWithSnapshot(timestamp.withMonth(7))
                );

        Flowable<Fragment> lastThreeBetweenAprilAndSeptember = doReadVersions(fragmentFlowable, Range.lastBetween(
                3, timestamp.withMonth(4), timestamp.withMonth(9)));
        assertThat(lastThreeBetweenAprilAndSeptember.blockingIterable())
                .containsExactly(
                        createFragmentWithSnapshot(timestamp.withMonth(8)),
                        createFragmentWithSnapshot(timestamp.withMonth(7)),
                        createFragmentWithSnapshot(timestamp.withMonth(6))
                );

        Flowable<Fragment> all = doReadVersions(fragmentFlowable, Range.unbounded());
        assertThat(all.blockingIterable()).containsExactly(fragments.toArray(Fragment[]::new));

        Flowable<Fragment> firstThree = doReadVersions(fragmentFlowable, Range.first(3));
        assertThat(firstThree.blockingIterable()).containsExactly(
                createFragmentWithSnapshot(timestamp.withMonth(1)),
                createFragmentWithSnapshot(timestamp.withMonth(2)),
                createFragmentWithSnapshot(timestamp.withMonth(3))
        );

        Flowable<Fragment> lastThree = doReadVersions(fragmentFlowable, Range.last(3));
        assertThat(lastThree.blockingIterable()).containsExactly(
                createFragmentWithSnapshot(timestamp.withMonth(12)),
                createFragmentWithSnapshot(timestamp.withMonth(11)),
                createFragmentWithSnapshot(timestamp.withMonth(10))
        );
    }

    @Test
    public void testFind() {
        // Test data. id00 to id11.
        SortedSet<Fragment> fragments = new TreeSet<>();
        for (int i = 0; i < 12; i++) {
            fragments.add(createFragmentWithIdAndPath("id" + String.format("%02d", i), "pathA"));
            fragments.add(createFragmentWithIdAndPath("id" + String.format("%02d", i), "pathB"));
        }
        Flowable<Fragment> fragmentFlowable = Flowable.fromIterable(fragments);

        Flowable<Fragment> firstSixAfterThree = doFind(fragmentFlowable, Range.firstAfter(6, "id03"));
        assertThat(firstSixAfterThree.blockingIterable())
                .containsExactly(
                        createFragmentWithIdAndPath("id04", "pathA"),
                        createFragmentWithIdAndPath("id04", "pathB"),
                        createFragmentWithIdAndPath("id05", "pathA"),
                        createFragmentWithIdAndPath("id05", "pathB"),
                        createFragmentWithIdAndPath("id06", "pathA"),
                        createFragmentWithIdAndPath("id06", "pathB")
                );

        Flowable<Fragment> lastSixBeforeTen = doFind(fragmentFlowable, Range.lastBefore(6, "id10"));
        assertThat(lastSixBeforeTen.blockingIterable())
                .containsExactly(
                        createFragmentWithIdAndPath("id09", "pathB"),
                        createFragmentWithIdAndPath("id09", "pathA"),
                        createFragmentWithIdAndPath("id08", "pathB"),
                        createFragmentWithIdAndPath("id08", "pathA"),
                        createFragmentWithIdAndPath("id07", "pathB"),
                        createFragmentWithIdAndPath("id07", "pathA")
                );

        Flowable<Fragment> firstThreeBetweenFourAndNine = doFind(fragmentFlowable, Range.firstBetween(
                6, "id04", "id09"));
        assertThat(firstThreeBetweenFourAndNine.blockingIterable())
                .containsExactly(
                        createFragmentWithIdAndPath("id05", "pathA"),
                        createFragmentWithIdAndPath("id05", "pathB"),
                        createFragmentWithIdAndPath("id06", "pathA"),
                        createFragmentWithIdAndPath("id06", "pathB"),
                        createFragmentWithIdAndPath("id07", "pathA"),
                        createFragmentWithIdAndPath("id07", "pathB")
                );

        Flowable<Fragment> lastThreeBetweenFourAndNine = doFind(fragmentFlowable, Range.lastBetween(
                6, "id04", "id09"));
        assertThat(lastThreeBetweenFourAndNine.blockingIterable())
                .containsExactly(
                        createFragmentWithIdAndPath("id08", "pathB"),
                        createFragmentWithIdAndPath("id08", "pathA"),
                        createFragmentWithIdAndPath("id07", "pathB"),
                        createFragmentWithIdAndPath("id07", "pathA"),
                        createFragmentWithIdAndPath("id06", "pathB"),
                        createFragmentWithIdAndPath("id06", "pathA")
                );

        Flowable<Fragment> all = doFind(fragmentFlowable, Range.unbounded());
        assertThat(all.blockingIterable()).containsExactly(fragments.toArray(Fragment[]::new));

        Flowable<Fragment> firstThree = doFind(fragmentFlowable, Range.first(3));
        assertThat(firstThree.blockingIterable()).containsExactly(
                createFragmentWithIdAndPath("id00", "pathA"),
                createFragmentWithIdAndPath("id00", "pathB"),
                createFragmentWithIdAndPath("id01", "pathA")
        );

        Flowable<Fragment> lastThree = doFind(fragmentFlowable, Range.last(3));
        assertThat(lastThree.blockingIterable()).containsExactly(
                createFragmentWithIdAndPath("id11", "pathB"),
                createFragmentWithIdAndPath("id11", "pathA"),
                createFragmentWithIdAndPath("id10", "pathB")
        );

    }

    @Test
    public void testDoReadAll() {
        // Test data. id00 to id11.
        SortedSet<Fragment> fragments = new TreeSet<>();
        for (int i = 0; i < 12; i++) {
            fragments.add(createFragmentWithId("id" + String.format("%02d", i)));
        }
        Flowable<Fragment> fragmentFlowable = Flowable.fromIterable(fragments);

        Flowable<Fragment> firstSixAfterThree = doReadAll(fragmentFlowable, Range.firstAfter(6, "id03"));
        assertThat(firstSixAfterThree.blockingIterable())
                .containsExactly(
                        createFragmentWithId("id04"),
                        createFragmentWithId("id05"),
                        createFragmentWithId("id06"),
                        createFragmentWithId("id07"),
                        createFragmentWithId("id08"),
                        createFragmentWithId("id09")
                );

        Flowable<Fragment> lastSixBeforeTen = doReadAll(fragmentFlowable, Range.lastBefore(6, "id10"));
        assertThat(lastSixBeforeTen.blockingIterable())
                .containsExactly(
                        createFragmentWithId("id09"),
                        createFragmentWithId("id08"),
                        createFragmentWithId("id07"),
                        createFragmentWithId("id06"),
                        createFragmentWithId("id05"),
                        createFragmentWithId("id04")
                );

        Flowable<Fragment> firstThreeBetweenFourAndNine = doReadAll(fragmentFlowable, Range.firstBetween(
                3, "id04", "id09"));
        assertThat(firstThreeBetweenFourAndNine.blockingIterable())
                .containsExactly(
                        createFragmentWithId("id05"),
                        createFragmentWithId("id06"),
                        createFragmentWithId("id07")
                );

        Flowable<Fragment> lastThreeBetweenFourAndNine = doReadAll(fragmentFlowable, Range.lastBetween(3, "id04", "id09"));
        assertThat(lastThreeBetweenFourAndNine.blockingIterable())
                .containsExactly(
                        createFragmentWithId("id08"),
                        createFragmentWithId("id07"),
                        createFragmentWithId("id06")
                );

        Flowable<Fragment> all = doReadAll(fragmentFlowable, Range.unbounded());
        assertThat(all.blockingIterable()).containsExactly(fragments.toArray(Fragment[]::new));

        Flowable<Fragment> firstThree = doReadAll(fragmentFlowable, Range.first(3));
        assertThat(firstThree.blockingIterable()).containsExactly(
                createFragmentWithId("id00"),
                createFragmentWithId("id01"),
                createFragmentWithId("id02")
        );

        Flowable<Fragment> lastThree = doReadVersions(fragmentFlowable, Range.last(3));
        assertThat(lastThree.blockingIterable()).containsExactly(
                createFragmentWithId("id11"),
                createFragmentWithId("id10"),
                createFragmentWithId("id09")
        );
    }
}