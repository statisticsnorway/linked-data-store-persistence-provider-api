package no.ssb.lds.api.persistence.flattened;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

public class PathComparatorTest {

    private TreeMap<String, String> map;

    @BeforeMethod
    public void setUp() {
        map = new TreeMap<>(new PathComparator());
    }

    @Test
    public void testSortSimple() {

        map.put("$.c.c", "");
        map.put("$.c.b", "");
        map.put("$.c.a", "");
        map.put("$.b.c", "");
        map.put("$.b.b", "");
        map.put("$.b.a", "");
        map.put("$.a.c", "");
        map.put("$.a.b", "");
        map.put("$.a.a", "");

        assertThat(map.keySet()).containsExactly(
                "$.a.a",
                "$.a.b",
                "$.a.c",
                "$.b.a",
                "$.b.b",
                "$.b.c",
                "$.c.a",
                "$.c.b",
                "$.c.c"
        );

    }

    @Test
    public void testSortIndices() {
        map.put("$.a[10].c", "");
        map.put("$.a[10].b", "");
        map.put("$.a[10].a", "");
        map.put("$.a[9].c", "");
        map.put("$.a[9].b", "");
        map.put("$.a[9].a", "");
        map.put("$.a[0].c", "");
        map.put("$.a[0].b", "");
        map.put("$.a[0].a", "");

        assertThat(map.keySet()).containsExactly(
                "$.a[0].a",
                "$.a[0].b",
                "$.a[0].c",

                "$.a[9].a",
                "$.a[9].b",
                "$.a[9].c",

                "$.a[10].a",
                "$.a[10].b",
                "$.a[10].c"
        );

    }

    @Test
    public void testSortLeaf() {
        map.put("$.a[9]", "");
        map.put("$.a[10]", "");
        map.put("$.a[0]", "");

        assertThat(map.keySet()).containsExactly(
                "$.a[0]",
                "$.a[9]",
                "$.a[10]"
        );

    }

    @Test
    public void testSortNested() {
        map.put("$.a[10].b[9].b", "");
        map.put("$.a[10].b[9].a", "");
        map.put("$.a[10].b[10].b", "");
        map.put("$.a[10].b[10].a", "");
        map.put("$.a[9].b[10].b", "");
        map.put("$.a[9].b[10].a", "");
        map.put("$.a[9].b[9].b", "");
        map.put("$.a[9].b[9].a", "");

        assertThat(map.keySet()).containsExactly(
                "$.a[9].b[9].a",
                "$.a[9].b[9].b",
                "$.a[9].b[10].a",
                "$.a[9].b[10].b",
                "$.a[10].b[9].a",
                "$.a[10].b[9].b",
                "$.a[10].b[10].a",
                "$.a[10].b[10].b"
        );

    }

    @Test
    public void testSortNestedLeaf() {
        map.put("$.a[10].b[10]", "");
        map.put("$.a[10].b[9]", "");
        map.put("$.a[9].b[10]", "");
        map.put("$.a[9].b[9]", "");

        assertThat(map.keySet()).containsExactly(
                "$.a[9].b[9]",
                "$.a[9].b[10]",
                "$.a[10].b[9]",
                "$.a[10].b[10]"
        );
    }
}