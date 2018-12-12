module no.ssb.lds.persistence.api {
    requires org.json;

    exports no.ssb.lds.api.persistence;
    exports no.ssb.lds.api.persistence.flattened;
    exports no.ssb.lds.api.persistence.streaming;
    exports no.ssb.lds.api.persistence.json;
    exports no.ssb.lds.api.specification;
}
