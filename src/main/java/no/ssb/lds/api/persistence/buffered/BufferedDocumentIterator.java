package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.PersistenceStatistics;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferedDocumentIterator implements Iterator<Document> {

    final List<Document> matches;
    final Iterator<Document> iterator;
    final PersistenceStatistics statistics;
    final AtomicBoolean limitedMatches = new AtomicBoolean(false);

    public BufferedDocumentIterator(List<Document> matches, PersistenceStatistics statistics) {
        this.matches = matches;
        this.statistics = statistics;
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

    public boolean hasMatchesBeenLimited() {
        return limitedMatches.get();
    }
}
