package no.ssb.lds.api.persistence.buffered;

import no.ssb.lds.api.persistence.PersistenceStatistics;

public class DocumentResult {

    protected final Document document;
    protected final PersistenceStatistics statistics;
    protected final Boolean limitedMatches;

    public DocumentResult(Document document, PersistenceStatistics statistics, Boolean limitedMatches) {
        this.document = document;
        this.statistics = statistics;
        this.limitedMatches = limitedMatches;
    }

    public Document document() {
        return document;
    }

    public PersistenceStatistics statistics() {
        return statistics;
    }

    public Boolean limitedMatches() {
        return limitedMatches;
    }
}
