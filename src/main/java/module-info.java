module no.ssb.lds.persistence.api {
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires com.github.akarnokd.rxjava2jdk9interop;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;

    exports no.ssb.lds.api.persistence;
    exports no.ssb.lds.api.persistence.flattened;
    exports no.ssb.lds.api.persistence.streaming;
    exports no.ssb.lds.api.persistence.json;
    exports no.ssb.lds.api.persistence.reactivex;
    exports no.ssb.lds.api.specification;
    exports no.ssb.lds.api.json;
}
