package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonToFlattenedDocument;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import org.assertj.core.api.Condition;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.time.ZonedDateTime.parse;
import static no.ssb.lds.api.persistence.reactivex.RxJsonPersistenceBridge.doReadDocument;
import static no.ssb.lds.api.persistence.reactivex.RxJsonPersistenceBridge.doReadDocuments;
import static org.assertj.core.api.Assertions.assertThat;

public class RxJsonPersistenceBridgeTest {

    private int capacity;

    private static Iterator<Fragment> createFragments(JsonDocument document, int capacity) {
        DocumentKey key = document.key();
        JSONObject json = document.document();
        JsonToFlattenedDocument flattenedDocument = new JsonToFlattenedDocument(
                key.namespace(), key.entity(), key.id(), key.timestamp(), json, capacity);

        return flattenedDocument.toDocument().fragmentIterator();
    }

    private static Condition<JsonDocument> thatIsEqualTo(JsonDocument document) {
        return new Condition<>(returnedDocument -> returnedDocument.key().equals(document.key())
                && returnedDocument.document().similar(document.document()), "similar documents");
    }

    @BeforeMethod
    public void setUp() {
        capacity = 128;
    }

    @Test
    public void testDoReadDocument() {

        JsonDocument document = createDocument("id");
        SortedSet<Fragment> fragments = new TreeSet<>();
        createFragments(document, capacity).forEachRemaining(fragments::add);
        Flowable<Fragment> fragmentFlowable = Flowable.fromIterable(fragments);

        Maybe<JsonDocument> map = doReadDocument(fragmentFlowable, capacity);
        assertThat(map.blockingGet()).is(thatIsEqualTo(document));
    }

    @Test
    public void testDoReadDocuments() {

        SortedSet<Fragment> fragments = new TreeSet<>();
        for (int i = 1; i < 12; i++) {
            JsonDocument document = createDocument(String.format("id%02d", i));
            createFragments(document, capacity).forEachRemaining(fragments::add);
        }
        Flowable<Fragment> fragmentFlowable = Flowable.fromIterable(fragments);

        Comparator<JsonDocument> byKeyAndJsonValue = Comparator
                .comparing(JsonDocument::key, (o1, o2) -> o1.equals(o2) ? 0 : -1)
                .thenComparing(JsonDocument::document, (o1, o2) -> o1.similar(o2) ? 0 : -1);

        Flowable<JsonDocument> betweenThreeAndNine = doReadDocuments(fragmentFlowable,
                Range.between("id03", "id09"), capacity);
        assertThat(betweenThreeAndNine.blockingIterable())
                .usingElementComparator(byKeyAndJsonValue)
                .containsExactly(
                        createDocument("id04"),
                        createDocument("id05"),
                        createDocument("id06"),
                        createDocument("id07"),
                        createDocument("id08")
                );
    }



    private JsonDocument createDocument(String id) {
        return new JsonDocument(
                new DocumentKey("ns", "entity", id, parse("2000-01-01T00:00:00.000Z")),
                createComplexObject(id)
        );
    }

    /**
     * Create an object that contains all values of {@link FragmentType}
     */
    private JSONObject createComplexObject(String id) {
        Map<String, Object> anObject = new LinkedHashMap<>();
        anObject.put("objectId", id);
        anObject.put("aTrue", true);
        anObject.put("aNumeric", 987654321.123456789);
        anObject.put("aString", "theString");
        anObject.put("anEmptyArray", List.of());
        anObject.put("anEmptyObject", Map.of());
        anObject.put("aFalse", false);
        // TODO: Seems JSONObject ignore null values.
        anObject.put("aNull", null);

        Map<String, Object> rootObject = new LinkedHashMap<>(anObject);
        rootObject.put("anObject", Map.of("firstObject", anObject, "secondObject", anObject));
        rootObject.put("anArray", List.of(anObject, anObject));
        return new JSONObject(rootObject);
    }
}