package no.ssb.lds.api.persistence.flattened;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class FlattenedDocumentTest {

    @Test
    public void thatLeafNodeWithSmallValueProduceSingleCorrectFragment() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        DocumentKey key = new DocumentKey("ns", "E", "1", now);
        TreeMap<String, FlattenedDocumentLeafNode> leafNodesByPath = new TreeMap<>();
        FlattenedDocument document = new FlattenedDocument(key, leafNodesByPath, false);
        FlattenedDocumentLeafNode node = new FlattenedDocumentLeafNode(key, "name", FragmentType.STRING, "My Name", 8 * 1024);
        leafNodesByPath.put("name", node);
        Iterator<Fragment> iterator = document.fragmentIterator();
        assertTrue(iterator.hasNext());
        Fragment fragment = iterator.next();
        Fragment expected = new Fragment("ns", "E", "1", now, "name", FragmentType.STRING, 0, "My Name".getBytes(StandardCharsets.UTF_8));
        assertEquals(fragment, expected);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void thatLeafNodeWithLargeValueProduceSeveralCorrectFragments() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        DocumentKey key = new DocumentKey("ns", "E", "1", now);
        FlattenedDocumentLeafNode firstname = new FlattenedDocumentLeafNode(key, "firstname", FragmentType.STRING, "John", 3);
        FlattenedDocumentLeafNode lastname = new FlattenedDocumentLeafNode(key, "lastname", FragmentType.STRING, "Smith", 3);
        TreeMap<String, FlattenedDocumentLeafNode> leafNodesByPath = new TreeMap<>();
        FlattenedDocument document = new FlattenedDocument(key, leafNodesByPath, false);
        leafNodesByPath.put("firstname", firstname);
        leafNodesByPath.put("lastname", lastname);
        Iterator<Fragment> iterator = document.fragmentIterator();

        assertTrue(iterator.hasNext());
        Fragment acutalFirstname0 = iterator.next();
        Fragment expectedFirstname0 = new Fragment("ns", "E", "1", now, "firstname", FragmentType.STRING, 0, "Joh".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalFirstname0, expectedFirstname0);
        assertTrue(iterator.hasNext());
        Fragment acutalFirstname3 = iterator.next();
        Fragment expectedFirstname3 = new Fragment("ns", "E", "1", now, "firstname", FragmentType.STRING, 3, "n".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalFirstname3, expectedFirstname3);

        assertTrue(iterator.hasNext());
        Fragment acutalLastname0 = iterator.next();
        Fragment expectedLastname0 = new Fragment("ns", "E", "1", now, "lastname", FragmentType.STRING, 0, "Smi".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalLastname0, expectedLastname0);
        assertTrue(iterator.hasNext());
        Fragment acutalLastname3 = iterator.next();
        Fragment expectedLastname3 = new Fragment("ns", "E", "1", now, "lastname", FragmentType.STRING, 3, "th".getBytes(StandardCharsets.UTF_8));
        assertEquals(acutalLastname3, expectedLastname3);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void thatDecodeDocumentWorks() {
        DocumentKey key = new DocumentKey("ns", "E", "1", ZonedDateTime.now(ZoneId.of("Etc/UTC")));

        Map<String, FlattenedDocumentLeafNode> leafNodesByPath = new LinkedHashMap<>();
        leafNodesByPath.put("firstname", new FlattenedDocumentLeafNode(key, "firstname", FragmentType.STRING, "John", 3));
        leafNodesByPath.put("lastname", new FlattenedDocumentLeafNode(key, "lastname", FragmentType.STRING, "Smith", 3));
        FlattenedDocument expectedDocument = new FlattenedDocument(key, leafNodesByPath, false);

        Map<String, List<Fragment>> fragmentsByPath = new LinkedHashMap<>();
        LinkedList<Fragment> fristnameFragments = new LinkedList<>();
        fragmentsByPath.put("firstname", fristnameFragments);
        LinkedList<Fragment> lastnameFragments = new LinkedList<>();
        fragmentsByPath.put("lastname", lastnameFragments);
        fristnameFragments.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "firstname", FragmentType.STRING, 0, "Joh".getBytes(StandardCharsets.UTF_8)));
        fristnameFragments.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "firstname", FragmentType.STRING, 3, "n".getBytes(StandardCharsets.UTF_8)));
        lastnameFragments.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "lastname", FragmentType.STRING, 0, "Smi".getBytes(StandardCharsets.UTF_8)));
        lastnameFragments.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "lastname", FragmentType.STRING, 3, "th".getBytes(StandardCharsets.UTF_8)));

        FlattenedDocument actual = FlattenedDocument.decodeDocument(key, fragmentsByPath, 3);

        assertEquals(actual, expectedDocument);
        assertEquals(actual.leafNodesByPath(), expectedDocument.leafNodesByPath());
    }
}
