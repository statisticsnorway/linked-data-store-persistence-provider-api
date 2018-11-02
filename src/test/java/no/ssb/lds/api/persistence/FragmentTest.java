package no.ssb.lds.api.persistence;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class FragmentTest {

    @Test
    public void thatArrayIndicesUnawarePathAndCorrectIndicesAreExtracted() {
        Fragment fragment = new Fragment("$.a.b[3103].c.d[2].e", "");
        Assert.assertEquals(fragment.getArrayIndicesUnawarePath(), "$.a.b[].c.d[].e");
        Assert.assertEquals(fragment.getArrayIndices(), List.of(3103, 2));
    }

    @Test
    public void thatArrayIndicesAndUnawarePathAreCorrectForEmptyPath() {
        Fragment fragment = new Fragment("", "");
        Assert.assertEquals(fragment.getArrayIndicesUnawarePath(), "");
        Assert.assertEquals(fragment.getArrayIndices(), List.of());
    }

    @Test
    public void thatArrayIndicesAndUnawarePathAreCorrectForPathWithoutArrays() {
        Fragment fragment = new Fragment("$.a.b.c", "");
        Assert.assertEquals(fragment.getArrayIndicesUnawarePath(), "$.a.b.c");
        Assert.assertEquals(fragment.getArrayIndices(), List.of());
    }

    @Test
    public void thatArrayIndicesAndUnawarePathAreCorrectForPathWithoutArrayIndices() {
        Fragment fragment = new Fragment("$.a.b[].c.d[0].e.f[].g", "");
        Assert.assertEquals(fragment.getArrayIndicesUnawarePath(), "$.a.b[].c.d[].e.f[].g");
        ArrayList<Integer> expected = new ArrayList<>();
        expected.add(null);
        expected.add(0);
        expected.add(null);
        Assert.assertEquals(fragment.getArrayIndices(), expected);
    }
}
