package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DocumentTest {

    @Test
    public void thatLeafNodeWithSmallValueProduceSingleCorrectFragment() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        DocumentKey key = new DocumentKey("ns", "E", "1", now);
        TreeMap<String, DocumentLeafNode> leafNodesByPath = new TreeMap<>();
        Document document = new Document(key, leafNodesByPath, false);
        DocumentLeafNode node = new DocumentLeafNode(key, "name", "My Name", 8 * 1024);
        leafNodesByPath.put("name", node);
        Iterator<Fragment> iterator = document.fragmentIterator();
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
        DocumentLeafNode firstname = new DocumentLeafNode(key, "firstname", "John", 3);
        DocumentLeafNode lastname = new DocumentLeafNode(key, "lastname", "Smith", 3);
        TreeMap<String, DocumentLeafNode> leafNodesByPath = new TreeMap<>();
        Document document = new Document(key, leafNodesByPath, false);
        leafNodesByPath.put("firstname", firstname);
        leafNodesByPath.put("lastname", lastname);
        Iterator<Fragment> iterator = document.fragmentIterator();

        assertTrue(iterator.hasNext());
        Fragment acutalFirstname0 = iterator.next();
        Fragment expectedFirstname0 = new Fragment("ns", "E", "1", now, "firstname", 0, "Joh".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalFirstname0, expectedFirstname0);
        assertTrue(iterator.hasNext());
        Fragment acutalFirstname3 = iterator.next();
        Fragment expectedFirstname3 = new Fragment("ns", "E", "1", now, "firstname", 3, "n".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalFirstname3, expectedFirstname3);

        assertTrue(iterator.hasNext());
        Fragment acutalLastname0 = iterator.next();
        Fragment expectedLastname0 = new Fragment("ns", "E", "1", now, "lastname", 0, "Smi".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalLastname0, expectedLastname0);
        assertTrue(iterator.hasNext());
        Fragment acutalLastname3 = iterator.next();
        Fragment expectedLastname3 = new Fragment("ns", "E", "1", now, "lastname", 3, "th".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalLastname3, expectedLastname3);

        assertFalse(iterator.hasNext());
    }
}
