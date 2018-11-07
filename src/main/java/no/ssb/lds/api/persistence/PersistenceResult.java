package no.ssb.lds.api.persistence;

import java.util.Collections;
import java.util.List;

public class PersistenceResult {
    final List<Document> matches;
    final boolean limitedMatches;
    final boolean readOnly;
    final PersistenceStatistics statistics;

    public static PersistenceResult writeResult() {
        return new PersistenceResult(Collections.emptyList(), false, false, new PersistenceStatistics());
    }

    public static PersistenceResult writeResult(PersistenceStatistics statistics) {
        return new PersistenceResult(Collections.emptyList(), false, false, statistics);
    }

    public static PersistenceResult readResult(Document match) {
        return new PersistenceResult(List.of(match), false, true, new PersistenceStatistics());
    }

    public static PersistenceResult readResult(Document match, PersistenceStatistics statistics) {
        return new PersistenceResult(List.of(match), false, true, statistics);
    }

    public static PersistenceResult readResult(List<Document> matches, boolean limitedMatches) {
        return new PersistenceResult(matches, limitedMatches, true, new PersistenceStatistics());
    }

    public static PersistenceResult readResult(List<Document> matches, boolean limitedMatches, PersistenceStatistics statistics) {
        return new PersistenceResult(matches, limitedMatches, true, statistics);
    }

    PersistenceResult(List<Document> matches, boolean limitedMatches, boolean readOnly, PersistenceStatistics statistics) {
        this.matches = matches;
        this.limitedMatches = limitedMatches;
        this.readOnly = readOnly;
        this.statistics = statistics;
    }

    public List<Document> getMatches() {
        return matches;
    }

    public boolean isLimitedMatches() {
        return limitedMatches;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public PersistenceStatistics getStatistics() {
        return statistics;
    }
}
