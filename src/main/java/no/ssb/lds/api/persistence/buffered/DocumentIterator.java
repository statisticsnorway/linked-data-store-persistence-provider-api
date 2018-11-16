package no.ssb.lds.api.persistence.buffered;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DocumentIterator implements Iterable<Document>, Iterator<Document> {

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

    /**
     * Returns a completable future that when complete signals whether or not the next() method will return another
     * element, and allows next to be called without blocking.
     * <p>
     * This should be used as an asynchronous callback when the iterator has more elements.
     *
     * @return
     */
    public CompletableFuture<Boolean> onHasNext() {
        return CompletableFuture.completedFuture(hasNext());
    }

    @Override
    public Iterator<Document> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Document next() {
        return iterator.next();
    }
}
