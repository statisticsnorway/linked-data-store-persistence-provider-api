package no.ssb.lds.api.persistence.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

public class BatchTest {

    @Test
    public void thatPutCatsWorks() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = getResourceAsString("batch/put_cats.json", StandardCharsets.UTF_8);
        testBatch(mapper, json);
    }

    @Test
    public void thatPutCatsAndDogsWorks() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = getResourceAsString("batch/put_cats_and_dogs.json", StandardCharsets.UTF_8);
        testBatch(mapper, json);
    }

    @Test
    public void thatDeleteSomeCatsWorks() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = getResourceAsString("batch/delete_some_cats.json", StandardCharsets.UTF_8);
        testBatch(mapper, json);
    }

    private void testBatch(ObjectMapper mapper, String json) throws IOException {
        JsonNode batchNode = mapper.readTree(json);
        Batch batch = new Batch(batchNode);
        for (Batch.Group group : batch.groups()) {
            System.out.printf("Group.Type : %s, Entity: %s%n", group.groupType(), group.type());
            if (group.groupType() == Batch.GroupType.PUT) {
                for (Batch.Entry entry : group.entries()) {
                    System.out.printf("ENTRY: id '%s'%n", entry.id);
                }
            } else if (group.groupType() == Batch.GroupType.DELETE) {
                MyBatchTestExpressionVisitor visitor = new MyBatchTestExpressionVisitor();
                group.evaluate(visitor);
                System.out.printf("%s%n", visitor.sb.toString());
            }
        }
    }

    static class MyBatchTestExpressionVisitor implements ExpressionVisitor {
        final StringBuilder sb = new StringBuilder();

        @Override
        public void enterMatch(ObjectNode matchNode) {
            sb.append("MATCH ");
        }

        @Override
        public void leaveMatch(ObjectNode matchNode) {
        }

        @Override
        public void enterAnd(ObjectNode andNode, boolean first, boolean last) {
            if (!first) {
                sb.append(" AND ");
            }
            sb.append("(");
        }

        @Override
        public void leaveAnd(ObjectNode andNode, boolean first, boolean last) {
            sb.append(")");
        }

        @Override
        public void enterOr(ObjectNode orNode, boolean first, boolean last) {
            if (!first) {
                sb.append(" OR ");
            }
            sb.append("(");
        }

        @Override
        public void leaveOr(ObjectNode orNode, boolean first, boolean last) {
            sb.append(")");
        }

        @Override
        public void enterNot(ObjectNode notNode) {
            sb.append("NOT (");
        }

        @Override
        public void leaveNot(ObjectNode notNode) {
            sb.append(")");
        }

        @Override
        public void enterIdIn(TextNode idInNode, boolean first, boolean last) {
            if (first && last) {
                sb.append("id = '").append(idInNode.textValue()).append("'");
            } else {
                if (first) {
                    sb.append("id IN [");
                } else {
                    sb.append(", ");
                }
                sb.append("'").append(idInNode.textValue()).append("'");
            }
        }

        @Override
        public void leaveIdIn(TextNode idInNode, boolean first, boolean last) {
            if (!first && last) {
                sb.append("]");
            }
        }

        @Override
        public void enterIdNotIn(TextNode idNotInNode, boolean first, boolean last) {
            if (first && last) {
                sb.append("id = '").append(idNotInNode.textValue()).append("'");
            } else {
                if (first) {
                    sb.append("id NOT IN [");
                } else {
                    sb.append(", ");
                }
                sb.append("'").append(idNotInNode.textValue()).append("'");
            }
        }

        @Override
        public void leaveIdNotIn(TextNode idNotInNode, boolean first, boolean last) {
            if (!first && last) {
                sb.append("]");
            }
        }

        @Override
        public void enterIdStartsWith(TextNode startsWithNode) {
            sb.append("STARTS_WITH '").append(startsWithNode.textValue()).append("'");
        }

        @Override
        public void leaveIdStartsWith(TextNode startsWithNode) {
        }
    }

    public static String getResourceAsString(String path, Charset charset) {
        try {
            URL systemResource = ClassLoader.getSystemResource(path);
            if (systemResource == null) {
                return null;
            }
            URLConnection conn = systemResource.openConnection();
            try (InputStream is = conn.getInputStream()) {
                byte[] bytes = is.readAllBytes();
                CharBuffer cbuf = CharBuffer.allocate(bytes.length);
                CoderResult coderResult = charset.newDecoder().decode(ByteBuffer.wrap(bytes), cbuf, true);
                if (coderResult.isError()) {
                    coderResult.throwException();
                }
                return cbuf.flip().toString();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}