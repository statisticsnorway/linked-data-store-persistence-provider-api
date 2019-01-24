module no.ssb.lds.persistence.api {
    requires org.json;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires com.github.akarnokd.rxjava2jdk9interop;

    exports no.ssb.lds.api.persistence;
    exports no.ssb.lds.api.persistence.flattened;
    exports no.ssb.lds.api.persistence.streaming;
    exports no.ssb.lds.api.persistence.json;
    exports no.ssb.lds.api.specification;
}
