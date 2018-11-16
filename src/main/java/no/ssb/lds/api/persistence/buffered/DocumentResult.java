package no.ssb.lds.api.persistence.buffered;

public class DocumentResult {

    final Document document;
    final Boolean limitedMatches;

    public DocumentResult(Document document, Boolean limitedMatches) {
        this.document = document;
        this.limitedMatches = limitedMatches;
    }

    public Document document() {
        return document;
    }

    public Boolean limitedMatches() {
        return limitedMatches;
    }
}
