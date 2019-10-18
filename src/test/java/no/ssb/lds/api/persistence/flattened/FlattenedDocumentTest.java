package no.ssb.lds.api.persistence.flattened;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static org.testng.Assert.*;

public class FlattenedDocumentTest {

    @Test
    public void thatLeafNodeWithSmallValueProduceSingleCorrectFragment() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        DocumentKey key = new DocumentKey("ns", "E", "1", now);
        TreeMap<String, FlattenedDocumentLeafNode> leafNodesByPath = new TreeMap<>();
        FlattenedDocumentLeafNode node = new FlattenedDocumentLeafNode(key, "name", FragmentType.STRING, "My Name", 8 * 1024);
        leafNodesByPath.put("name", node);
        FlattenedDocument document = new FlattenedDocument(key, leafNodesByPath, false);
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
        leafNodesByPath.put("firstname", firstname);
        leafNodesByPath.put("lastname", lastname);
        FlattenedDocument document = new FlattenedDocument(key, leafNodesByPath, false);
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

    @Test
    public void thatDecodeDocumentWithComplexArrayWorks() {
        DocumentKey key = new DocumentKey("ns", "E", "1", ZonedDateTime.now(ZoneId.of("Etc/UTC")));

        Map<String, FlattenedDocumentLeafNode> leafNodesByPath = new LinkedHashMap<>();
        leafNodesByPath.put("$.name[0].first", new FlattenedDocumentLeafNode(key, "$.name[0].first", FragmentType.STRING, "John", 64));
        leafNodesByPath.put("$.name[0].last", new FlattenedDocumentLeafNode(key, "$.name[0].last", FragmentType.STRING, "Smith", 64));
        leafNodesByPath.put("$.name[1].first", new FlattenedDocumentLeafNode(key, "$.name[1].first", FragmentType.STRING, "Jane", 64));
        leafNodesByPath.put("$.name[1].last", new FlattenedDocumentLeafNode(key, "$.name[1].last", FragmentType.STRING, "Doe", 64));
        FlattenedDocument expectedDocument = new FlattenedDocument(key, leafNodesByPath, false);

        Map<String, List<Fragment>> fragmentsByPath = new LinkedHashMap<>();
        List<Fragment> name0First = new ArrayList<>();
        List<Fragment> name0Last = new ArrayList<>();
        List<Fragment> name1First = new ArrayList<>();
        List<Fragment> name1Last = new ArrayList<>();
        fragmentsByPath.put("$.name[0].first", name0First);
        fragmentsByPath.put("$.name[0].last", name0Last);
        fragmentsByPath.put("$.name[1].first", name1First);
        fragmentsByPath.put("$.name[1].last", name1Last);
        name0First.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "$.name[0].first", FragmentType.STRING, 0, "John".getBytes(StandardCharsets.UTF_8)));
        name0Last.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "$.name[0].last", FragmentType.STRING, 0, "Smith".getBytes(StandardCharsets.UTF_8)));
        name1First.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "$.name[1].first", FragmentType.STRING, 0, "Jane".getBytes(StandardCharsets.UTF_8)));
        name1Last.add(new Fragment(key.namespace(), key.entity(), key.id(), key.timestamp(), "$.name[1].last", FragmentType.STRING, 0, "Doe".getBytes(StandardCharsets.UTF_8)));

        FlattenedDocument actual = FlattenedDocument.decodeDocument(key, fragmentsByPath, 64);

        assertEquals(actual, expectedDocument);
        assertEquals(actual.leafNodesByPath(), expectedDocument.leafNodesByPath());
    }
}
