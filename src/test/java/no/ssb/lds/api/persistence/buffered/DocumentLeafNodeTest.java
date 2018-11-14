package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DocumentLeafNodeTest {

    @Test
    public void thatLeafNodeWithSmallValueProduceSingleCorrectFragment() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        DocumentKey key = new DocumentKey("ns", "E", "1", now);
        DocumentLeafNode node = new DocumentLeafNode(key, "name", "My Name", 8 * 1024);
        Iterator<Fragment> iterator = node.fragmentIterator();
        assertTrue(iterator.hasNext());
        Fragment fragment = iterator.next();
        Fragment expected = new Fragment("ns", "E", "1", now, "name", 0, "My Name".getBytes(StandardCharsets.UTF_8));
        assertEquals(fragment, expected);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void thatLeafNodeWithLargeValueProduceSeveralCorrectFragments() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        DocumentKey key = new DocumentKey("ns", "E", "1", now);
        DocumentLeafNode node = new DocumentLeafNode(key, "name", "My Name", 3);
        Iterator<Fragment> iterator = node.fragmentIterator();
        assertTrue(iterator.hasNext());
        Fragment acutal0 = iterator.next();
        Fragment expected0 = new Fragment("ns", "E", "1", now, "name", 0, "My ".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutal0, expected0);
        assertTrue(iterator.hasNext());
        Fragment acutal3 = iterator.next();
        Fragment expected3 = new Fragment("ns", "E", "1", now, "name", 3, "Nam".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutal3, expected3);
        assertTrue(iterator.hasNext());
        Fragment acutal6 = iterator.next();
        Fragment expected6 = new Fragment("ns", "E", "1", now, "name", 6, "e".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutal6, expected6);
        assertFalse(iterator.hasNext());
    }
}
