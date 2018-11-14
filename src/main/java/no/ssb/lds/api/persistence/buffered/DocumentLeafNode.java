package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.Fragment;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class DocumentLeafNode {

    final DocumentKey key;
    final String path;
    final String value;

    public DocumentLeafNode(DocumentKey key, String path, String value) {
        this.key = key;
        this.path = path;
        this.value = value;
    }

    public DocumentKey key() {
        return key;
    }

    public String path() {
        return path;
    }

    public String value() {
        return value;
    }

    Iterator<Fragment> fragmentIterator() {
        List<Fragment> fragments = new LinkedList<>();
        ByteBuffer out = ByteBuffer.allocate(8 * 1024);
        CharBuffer in = CharBuffer.wrap(value);
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
        CoderResult coderResult;
        coderResult = encoder.encode(in, out, false);
        handleError(coderResult);
        int offset = 0;
        while (coderResult.isOverflow()) {
            byte[] fragmentValue = new byte[0];
            System.arraycopy(out.array(), 0, fragmentValue, 0, out.position());
            fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, offset, fragmentValue));
            offset += out.position();
            out.clear();
            coderResult = encoder.encode(in, out, false);
            handleError(coderResult);
        }
        // underflow
        CoderResult endOfInputCoderResult = encoder.encode(in, out, true);
        handleError(endOfInputCoderResult);
        CoderResult flushCoderResult = encoder.flush(out);
        handleError(flushCoderResult);
        fragments.add(new Fragment(key.namespace, key.entity, key.id, key.timestamp, path, offset, out.array()));
        return fragments.iterator();
    }

    static void handleError(CoderResult coderResult) {
        if (coderResult.isError()) {
            try {
                coderResult.throwException();
                throw new IllegalStateException();
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
