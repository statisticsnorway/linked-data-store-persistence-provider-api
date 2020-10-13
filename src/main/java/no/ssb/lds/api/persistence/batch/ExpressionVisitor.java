package no.ssb.lds.api.persistence.batch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public interface ExpressionVisitor {

    void enterMatch(ObjectNode matchNode);

    void leaveMatch(ObjectNode matchNode);

    void enterAnd(ObjectNode andNode, boolean first, boolean last);

    void leaveAnd(ObjectNode andNode, boolean first, boolean last);

    void enterOr(ObjectNode orNode, boolean first, boolean last);

    void leaveOr(ObjectNode orNode, boolean first, boolean last);

    void enterNot(ObjectNode notNode);

    void leaveNot(ObjectNode notNode);

    void enterIdIn(TextNode idInNode, boolean first, boolean last);

    void leaveIdIn(TextNode idInNode, boolean first, boolean last);

    void enterIdNotIn(TextNode idNotInNode, boolean first, boolean last);

    void leaveIdNotIn(TextNode idNotInNode, boolean first, boolean last);

    void enterIdStartsWith(TextNode startsWithNode);

    void leaveIdStartsWith(TextNode startsWithNode);
}
