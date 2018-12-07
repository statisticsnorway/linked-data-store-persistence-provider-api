package no.ssb.lds.api.persistence.streaming;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class FragmentTest {

    @Test
    public void thatArrayIndicesUnawarePathAndCorrectIndicesAreExtracted() {
        Fragment fragment = new Fragment("", "", "", ZonedDateTime.now(ZoneId.of("Etc/UTC")), "$.a.b[3103].c.d[2].e", FragmentType.NULL, 0, null);
        ArrayList<Integer> indices = new ArrayList<>();
        String actualPath = Fragment.computeIndexUnawarePath(fragment.path, indices);
        Assert.assertEquals(actualPath, "$.a.b[].c.d[].e");
        Assert.assertEquals(indices, List.of(3103, 2));
    }

    @Test
    public void thatArrayIndicesAndUnawarePathAreCorrectForEmptyPath() {
        Fragment fragment = new Fragment("", "", "", ZonedDateTime.now(ZoneId.of("Etc/UTC")), "", FragmentType.NULL, 0, null);
        ArrayList<Integer> indices = new ArrayList<>();
        String actualPath = Fragment.computeIndexUnawarePath(fragment.path, indices);
        Assert.assertEquals(actualPath, "");
        Assert.assertEquals(indices, List.of());
    }

    @Test
    public void thatArrayIndicesAndUnawarePathAreCorrectForPathWithoutArrays() {
        Fragment fragment = new Fragment("", "", "", ZonedDateTime.now(ZoneId.of("Etc/UTC")), "$.a.b.c", FragmentType.NULL, 0, null);
        ArrayList<Integer> indices = new ArrayList<>();
        String actualPath = Fragment.computeIndexUnawarePath(fragment.path, indices);
        Assert.assertEquals(actualPath, "$.a.b.c");
        Assert.assertEquals(indices, List.of());
    }

    @Test
    public void thatArrayIndicesAndUnawarePathAreCorrectForPathWithoutArrayIndices() {
        Fragment fragment = new Fragment("", "", "", ZonedDateTime.now(ZoneId.of("Etc/UTC")), "$.a.b[].c.d[0].e.f[].g", FragmentType.NULL, 0, null);
        ArrayList<Integer> indices = new ArrayList<>();
        String actualPath = Fragment.computeIndexUnawarePath(fragment.path, indices);
        Assert.assertEquals(actualPath, "$.a.b[].c.d[].e.f[].g");
        ArrayList<Integer> expected = new ArrayList<>();
        expected.add(null);
        expected.add(0);
        expected.add(null);
        Assert.assertEquals(indices, expected);
    }
}
