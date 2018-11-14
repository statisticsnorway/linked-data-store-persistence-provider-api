package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.PersistenceStatistics;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferedDocumentIterator implements Iterator<Document> {

    final List<Document> matches;
    final AtomicBoolean hasNext;
    final PersistenceStatistics statistics;
    final AtomicBoolean limitedMatches = new AtomicBoolean(false);

    public BufferedDocumentIterator(List<Document> matches, PersistenceStatistics statistics) {
        this.matches = matches;
        this.statistics = statistics;
        hasNext = new AtomicBoolean(matches != null && !matches.isEmpty());
    }

    public CompletableFuture<Boolean> onHasNext() {
        return CompletableFuture.completedFuture(hasNext.get());
    }

    @Override
    public boolean hasNext() {
        return hasNext.get();
    }

    @Override
    public Document next() {
        if (hasNext.compareAndSet(true, false)) {
            return matches.get(0);
        }
        throw new NoSuchElementException();
    }

    public void cancel() {
    }

    public boolean hasMatchesBeenLimited() {
        return false;
    }
}
