package no.ssb.lds.api.json;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class JsonNavigationPathTest {

    @Test
    public void thatDeserializationAndSerializationAreCompatible() {
        assertEquals(JsonNavigationPath.from("$").serialize(), "$");
        assertEquals(JsonNavigationPath.from("$[]").serialize(), "$[]");
        assertEquals(JsonNavigationPath.from("$[1]").serialize(), "$[1]");
        assertEquals(JsonNavigationPath.from("$[][]").serialize(), "$[][]");
        assertEquals(JsonNavigationPath.from("$.a.b.c").serialize(), "$.a.b.c");
        assertEquals(JsonNavigationPath.from("$.singleTopLevelField").serialize(), "$.singleTopLevelField");
        assertEquals(JsonNavigationPath.from("$.a[].b[][].c.d[][][]").serialize(), "$.a[].b[][].c.d[][][]");
        assertEquals(JsonNavigationPath.from("$.a[].b[][].c.d[][][].x.y").serialize(), "$.a[].b[][].c.d[][][].x.y");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatArrayElementByItselfIsIllegal() {
        JsonNavigationPath.from("$.[].a");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatArrayElementMalformed1IsIllegal() {
        JsonNavigationPath.from("$.[]x");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatArrayElementMalformed2IsIllegal() {
        JsonNavigationPath.from("$.x[");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatArrayElementMalformed3IsIllegal() {
        JsonNavigationPath.from("$.x]");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatArrayElementMalformed4IsIllegal() {
        JsonNavigationPath.from("$.x[]y");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatArrayElementMalformed5IsIllegal() {
        JsonNavigationPath.from("$.x[aw]y");
    }
}
