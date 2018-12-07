package no.ssb.lds.api.persistence.buffered;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FlattenedDocumentIterator implements Iterable<FlattenedDocument>, Iterator<FlattenedDocument> {

    final List<FlattenedDocument> matches;
    final Iterator<FlattenedDocument> iterator;

    public FlattenedDocumentIterator(List<FlattenedDocument> matches) {
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
    public Iterator<FlattenedDocument> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public FlattenedDocument next() {
        return iterator.next();
    }
}
