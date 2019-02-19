package no.ssb.lds.api.persistence.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONException;
import org.testng.annotations.Test;

import static no.ssb.lds.api.persistence.json.JsonTools.mapper;
import static no.ssb.lds.api.persistence.json.JsonTools.toJson;
import static no.ssb.lds.api.persistence.json.JsonTools.toJsonNode;
import static no.ssb.lds.api.persistence.json.JsonTools.toList;
import static no.ssb.lds.api.persistence.json.JsonTools.toMap;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

public class JsonToolsTest {

    @Test
    public void JsonConversionRoundTrip() throws JSONException {
        ObjectNode root = mapper.createObjectNode();
        populateWithPrimitives(root);
        ObjectNode objectFoo = root.putObject("objectFoo");
        populateWithPrimitives(objectFoo);
        ArrayNode arrayFoo = root.putArray("arrayFoo");
        arrayFoo.add("bar1");
        arrayFoo.add("bar2");
        ArrayNode arrayParent = root.putArray("arrayParent");
        ArrayNode arrayChild1 = arrayParent.addArray();
        populateWithPrimitives(arrayChild1.addObject());
        populateWithPrimitives(arrayChild1.addObject());
        ArrayNode arrayChild2 = arrayParent.addArray();
        populateWithPrimitives(arrayChild2.addObject());
        populateWithPrimitives(arrayChild2.addObject());

        assertEquals(toJson(toJsonNode(toMap(root))), toJson(root), true);
        assertEquals(toJson(toJsonNode(toList(arrayParent))), toJson(arrayParent), true);
    }

    private void populateWithPrimitives(ObjectNode node) {
        node.put("fooStr", "bar");
        node.put("fooInt", 1);
        node.put("fooLong", 2L);
        node.put("fooFloat", 2.3f);
        node.put("fooDouble", 2.3);
        node.put("fooBoolean", true);
    }
}
