package no.ssb.lds.api.persistence;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PersistenceStatistics {
    final Map<String, AtomicLong> statistics = new ConcurrentHashMap<>();

    public PersistenceStatistics add(String statistic, int increment) {
        statistics.computeIfAbsent(statistic, s -> new AtomicLong(0)).addAndGet(increment);
        return this;
    }

    public Map<String, Long> map() {
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<String, AtomicLong> e : statistics.entrySet()) {
            result.put(e.getKey(), e.getValue().get());
        }
        return result;
    }

    @Override
    public String toString() {
        return "PersistenceStatistics{" +
                "statistics=" + statistics +
                '}';
    }
}
