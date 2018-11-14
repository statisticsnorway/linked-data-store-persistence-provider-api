package no.ssb.lds.api.persistence;

public class PersistenceResult {

    protected final Fragment fragment;
    protected final PersistenceStatistics statistics;
    protected final Boolean limitedMatches;

    public PersistenceResult(Fragment fragment, PersistenceStatistics statistics) {
        this.fragment = fragment;
        this.limitedMatches = null;
        this.statistics = statistics;
    }

    public PersistenceResult(Fragment fragment, PersistenceStatistics statistics, boolean limitedMatches) {
        this.fragment = fragment;
        this.limitedMatches = limitedMatches;
        this.statistics = statistics;
    }

    public Fragment fragment() {
        return fragment;
    }

    public Boolean limitedMatches() {
        return limitedMatches;
    }

    public PersistenceStatistics statistics() {
        return statistics;
    }
}
