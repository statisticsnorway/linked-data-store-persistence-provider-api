package no.ssb.lds.api.persistence.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Batch {

    final JsonNode batchNode;
    final List<Group> groups = new ArrayList<>();

    public Batch(JsonNode batchNode) {
        this.batchNode = batchNode;
        if (batchNode.isArray()) {
            Iterator<JsonNode> elements = batchNode.elements();
            while (elements.hasNext()) {
                JsonNode groupNode = elements.next();
                addGroup(groupNode);
            }
        } else if (batchNode.isObject()) {
            addGroup(batchNode);
        }
    }

    private void addGroup(JsonNode groupNode) {
        JsonNode operationNode = groupNode.get("operation");
        String operation = operationNode.textValue();
        if ("put".equals(operation)) {
            groups.add(new PutGroup(groupNode));
        } else if ("delete".equals(operation)) {
            groups.add(new DeleteGroup(groupNode));
        } else {
            throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    public List<Group> groups() {
        return Collections.unmodifiableList(groups);
    }

    public JsonNode getBatchNode() {
        return batchNode;
    }

    public enum GroupType {
        PUT, DELETE
    }

    public static abstract class Group {
        final JsonNode groupNode;
        final String type;

        Group(JsonNode groupNode) {
            this.groupNode = groupNode;
            this.type = groupNode.get("type").textValue();
        }

        public abstract GroupType groupType();

        public String type() {
            return type;
        }

        public JsonNode getGroupNode() {
            return groupNode;
        }

        public List<Entry> entries() {
            if (!groupNode.has("entries")) {
                return Collections.emptyList();
            }
            List<Entry> result = new ArrayList<>();
            ArrayNode entries = (ArrayNode) groupNode.get("entries");
            for (JsonNode node : entries) {
                result.add(new Entry((ObjectNode) node));
            }
            return result;
        }

        public abstract boolean hasMatchCriteria();

        public abstract void evaluate(ExpressionVisitor visitor);
    }

    public static class PutGroup extends Group {
        PutGroup(JsonNode groupNode) {
            super(groupNode);
            ArrayNode entries = (ArrayNode) groupNode.get("entries");
        }

        @Override
        public GroupType groupType() {
            return GroupType.PUT;
        }

        @Override
        public boolean hasMatchCriteria() {
            return false;
        }

        @Override
        public void evaluate(ExpressionVisitor visitor) {
            throw new UnsupportedOperationException();
        }
    }

    public static class Entry {
        final String id;
        final ZonedDateTime timestamp;
        final JsonNode dataNode;

        Entry(ObjectNode entryNode) {
            this.id = entryNode.get("id").textValue();
            this.timestamp = ZonedDateTime.parse(entryNode.get("timestamp").textValue());
            this.dataNode = entryNode.get("data");
        }

        public String id() {
            return id;
        }

        public ZonedDateTime timestamp() {
            return timestamp;
        }

        public JsonNode dataNode() {
            return dataNode;
        }
    }

    public static class DeleteGroup extends Group {
        final JsonNode matchNode;
        final ZonedDateTime timestamp;

        DeleteGroup(JsonNode groupNode) {
            super(groupNode);
            matchNode = groupNode.has("match") ? groupNode.get("match") : null;
            this.timestamp = groupNode.has("timestamp") ? ZonedDateTime.parse(groupNode.get("timestamp").textValue()) : null;
        }

        public ZonedDateTime getTimestamp() {
            return timestamp;
        }

        @Override
        public GroupType groupType() {
            return GroupType.DELETE;
        }

        @Override
        public boolean hasMatchCriteria() {
            return groupNode.has("match") && (groupNode.get("match") instanceof ObjectNode);
        }

        @Override
        public void evaluate(ExpressionVisitor visitor) {
            if (!hasMatchCriteria()) {
                return;
            }
            visitor.enterMatch((ObjectNode) matchNode);
            traverse((ObjectNode) matchNode, visitor);
            visitor.leaveMatch((ObjectNode) matchNode);
        }
    }

    static void traverse(ObjectNode node, ExpressionVisitor visitor) {
        if (node.has("and")) {
            ArrayNode andNode = (ArrayNode) node.get("and");
            Iterator<JsonNode> elementsIterator = andNode.elements();
            boolean first = true;
            while (elementsIterator.hasNext()) {
                ObjectNode element = (ObjectNode) elementsIterator.next();
                visitor.enterAnd(element, first, !elementsIterator.hasNext());
                traverse(element, visitor);
                visitor.leaveAnd(element, first, !elementsIterator.hasNext());
                first = false;
            }
            return;
        }
        if (node.has("or")) {
            ArrayNode orNode = (ArrayNode) node.get("or");
            Iterator<JsonNode> elementsIterator = orNode.elements();
            boolean first = true;
            while (elementsIterator.hasNext()) {
                ObjectNode element = (ObjectNode) elementsIterator.next();
                visitor.enterOr(element, first, !elementsIterator.hasNext());
                traverse(element, visitor);
                visitor.leaveOr(element, first, !elementsIterator.hasNext());
                first = false;
            }
            return;
        }
        if (node.has("not")) {
            ObjectNode notNode = (ObjectNode) node.get("not");
            visitor.enterNot(notNode);
            traverse(notNode, visitor);
            visitor.leaveNot(notNode);
            return;
        }
        if (node.has("id-starts-with")) {
            TextNode idStartsWithNode = (TextNode) node.get("id-starts-with");
            visitor.enterIdStartsWith(idStartsWithNode);
            visitor.leaveIdStartsWith(idStartsWithNode);
            return;
        }
        if (node.has("id-in")) {
            ArrayNode idInNode = (ArrayNode) node.get("id-in");
            Iterator<JsonNode> elementsIterator = idInNode.elements();
            boolean first = true;
            while (elementsIterator.hasNext()) {
                TextNode element = (TextNode) elementsIterator.next();
                visitor.enterIdIn(element, first, !elementsIterator.hasNext());
                visitor.leaveIdIn(element, first, !elementsIterator.hasNext());
                first = false;
            }
            return;
        }
        if (node.has("id-not-in")) {
            ArrayNode idInNode = (ArrayNode) node.get("id-not-in");
            Iterator<JsonNode> elementsIterator = idInNode.elements();
            boolean first = true;
            while (elementsIterator.hasNext()) {
                TextNode element = (TextNode) elementsIterator.next();
                visitor.enterIdNotIn(element, first, !elementsIterator.hasNext());
                visitor.leaveIdNotIn(element, first, !elementsIterator.hasNext());
                first = false;
            }
            return;
        }
        if (node.has("id")) {
            TextNode idNode = (TextNode) node.get("id");
            visitor.enterIdIn(idNode, true, true);
            visitor.leaveIdIn(idNode, true, true);
            return;
        }
    }

}
