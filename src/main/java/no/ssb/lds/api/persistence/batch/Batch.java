package no.ssb.lds.api.persistence.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Batch {

    final JsonNode batchNode;
    final ZonedDateTime now;
    final List<Group> groups = new ArrayList<>();

    public Batch(JsonNode batchNode) {
        this.batchNode = batchNode;
        this.now = ZonedDateTime.now(ZoneOffset.UTC); // default timestamp in case non is supplied in batch
        if (batchNode.isArray()) {
            Iterator<JsonNode> elements = batchNode.elements();
            while (elements.hasNext()) {
                JsonNode groupNode = elements.next();
                addGroup((ObjectNode) groupNode);
            }
        } else if (batchNode.isObject()) {
            addGroup((ObjectNode) batchNode);
        }
    }

    private void addGroup(ObjectNode groupNode) {
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

    public abstract class Group {
        final ObjectNode groupNode;
        final String type;

        Group(ObjectNode groupNode) {
            this.groupNode = groupNode;
            this.type = groupNode.get("type").textValue();
            entries(); // force use of timestamp in nodes
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
                result.add(new Entry(groupNode, (ObjectNode) node));
            }
            return result;
        }

        public abstract boolean hasMatchCriteria();

        public abstract void evaluate(ExpressionVisitor visitor);
    }

    public class PutGroup extends Group {
        PutGroup(ObjectNode groupNode) {
            super(groupNode);
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

    public class Entry {
        final String id;
        final ZonedDateTime timestamp;
        final JsonNode dataNode;

        Entry(ObjectNode groupNode, ObjectNode entryNode) {
            this.id = entryNode.get("id").textValue();
            if (entryNode.has("timestamp") && entryNode.get("timestamp").isTextual()) {
                this.timestamp = ZonedDateTime.parse(entryNode.get("timestamp").textValue());
            } else if (groupNode.has("timestamp") && groupNode.get("timestamp").isTextual()) {
                this.timestamp = ZonedDateTime.parse(groupNode.get("timestamp").textValue());
            } else {
                this.timestamp = Batch.this.now;
                groupNode.put("timestamp", this.timestamp.toString());
            }
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

    public class DeleteGroup extends Group {
        final ObjectNode matchNode;
        final ZonedDateTime timestamp;

        DeleteGroup(ObjectNode groupNode) {
            super(groupNode);
            matchNode = groupNode.has("match") ? (ObjectNode) groupNode.get("match") : null;
            if (groupNode.has("timestamp") && groupNode.get("timestamp").isTextual()) {
                this.timestamp = ZonedDateTime.parse(groupNode.get("timestamp").textValue());
            } else {
                this.timestamp = null;
            }
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
            visitor.enterMatch(matchNode);
            traverse(matchNode, visitor);
            visitor.leaveMatch(matchNode);
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
