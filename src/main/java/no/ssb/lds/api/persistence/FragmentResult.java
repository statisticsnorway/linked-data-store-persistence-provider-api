package no.ssb.lds.api.persistence;

public class FragmentResult {

    protected final Fragment fragment;
    protected final TransactionStatistics statistics;
    protected final boolean limitedMatches;

    public FragmentResult(Fragment fragment, TransactionStatistics statistics, boolean limitedMatches) {
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

    public TransactionStatistics statistics() {
        return statistics;
    }
}
