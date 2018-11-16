package no.ssb.lds.api.persistence.buffered;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DocumentIterator implements Iterator<Document> {

    final List<Document> matches;
    final Iterator<Document> iterator;

    public DocumentIterator(List<Document> matches) {
        this.matches = matches;
        if (matches == null) {
            this.iterator = Collections.emptyListIterator();
        } else {
            this.iterator = matches.iterator();
        }
    }

    public CompletableFuture<Boolean> onHasNext() {
        return CompletableFuture.completedFuture(hasNext());
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Document next() {
        return iterator.next();
    }

    public void cancel() {
    }
}
