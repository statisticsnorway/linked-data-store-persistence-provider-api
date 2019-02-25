package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Flowable;
import io.reactivex.Maybe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

public class RxGroupByTools {

    /**
     * Assumes that the upstream is pre-ordered so that all upstream items that yield the same output when applying the
     * groupKeyFunction will come immediately after one-another. This allows the flow to only buffer upstream items from
     * a single group at any time, thus allowing much more control over memory usage, and given finite groups will avoid
     * unbounded memory usage.
     * <p>
     * This function honors downstream back-pressure and will only consume as many groups upstream as back-pressure
     * allows downstream.
     *
     * @param upstream           the upstream as a flow of upstream-items
     * @param groupKeyFunction   the function that will be applied to an upstream item to determine which group it
     *                           belongs to. This function must be idempotent (cannot have side effect as it might be
     *                           applied more than once).
     * @param convertionFunction a function that given a group-key and the corresponding buffer of all belonging
     *                           upstream-items will produce a single downstream item.
     * @param <D>                the generic type of the downstream items.
     * @param <U>                the generic type of the upstream items.
     * @param <UK>               the generic type of the group-key. This type must implement the equals and hashCode
     *                           methods so that the group-key of two different upstream-items (as provided by the
     *                           groupKeyFunction) that belong in the same group will be considered equal, otherwise (if
     *                           they belong in different groups will be considered not-equals.
     * @return a Flowable flow of downstream items.
     */
    public static <D, U, UK> Flowable<D> groupByConvertOrdered(final Flowable<U> upstream, final Function<U, UK> groupKeyFunction, BiFunction<UK, List<U>, D> convertionFunction) {

        final AtomicReference<UK> groupKeyRef = new AtomicReference<>();
        final List<U> itemBuffer = new ArrayList<>();
        final AtomicReference<CompletableFuture<D>> upstreamCompleteSignal = new AtomicReference<>();

        return upstream.filter(upstreamItem -> {
            // Group by object determined by groupKeyFunction
            UK groupKey = groupKeyFunction.apply(upstreamItem);
            if (groupKeyRef.get() == null) {
                // very first upstream item
                groupKeyRef.set(groupKey);
            }
            if (groupKey.equals(groupKeyRef.get())) {
                // upstream item belongs in current group, buffer item
                itemBuffer.add(upstreamItem);
                return false; // do not pass item downstream
            }
            if (itemBuffer.isEmpty()) {
                // empty item group, replace group using current item
                itemBuffer.add(upstreamItem);
                groupKeyRef.set(groupKey);
                return false; // do not pass item downstream
            }
            // new group
            return true; // pass item downstream

        }).doOnComplete(() -> {
            if (itemBuffer.isEmpty()) {
                // upstream flow contained no items
                groupKeyRef.set(null);
                upstreamCompleteSignal.get().complete(null);
                return;
            }
            D downstreamItem = convertionFunction.apply(groupKeyRef.get(), itemBuffer);
            itemBuffer.clear();
            groupKeyRef.set(null);
            upstreamCompleteSignal.get().complete(downstreamItem);

        }).doOnCancel(() -> {
            // reset state in-case someone re-subscribes to this flow
            itemBuffer.clear();
            groupKeyRef.set(null);
            upstreamCompleteSignal.get().complete(null); // complete merge with no item

        }).map(upstreamItem -> {
            D downstreamItem = convertionFunction.apply(groupKeyRef.get(), itemBuffer);
            itemBuffer.clear();
            itemBuffer.add(upstreamItem);
            groupKeyRef.set(groupKeyFunction.apply(upstreamItem));
            return downstreamItem;

        }).compose(upstreamFlowable -> {
            upstreamCompleteSignal.set(new CompletableFuture<>());
            return upstreamFlowable.mergeWith(Maybe.using(() -> upstreamCompleteSignal.get(),
                    d -> Maybe.fromFuture(d),
                    d -> upstreamCompleteSignal.set(new CompletableFuture<>()))
            );

        });
    }

    static AtomicInteger nextFlowId = new AtomicInteger(1);

    public static <T> Flowable<T> debug(final Flowable<T> flowable, final String flowDescription) {
        final int flowId = nextFlowId.getAndIncrement();

        return flowable

                // terminate

                .doOnComplete(() -> {
                    System.out.printf("%d %s onComplete%n", flowId, flowDescription);
                })
                .doOnError(throwable -> {
                    System.out.printf("%d %s onError: %s%n", flowId, flowDescription, throwable.getMessage());
                })


                //lifecycle

                .doOnSubscribe(subscription -> {
                    System.out.printf("%d %s onSubscribe: %s%n", flowId, flowDescription, subscription.toString());
                })
                .doOnCancel(() -> {
                    System.out.printf("%d %s onCancel%n", flowId, flowDescription);
                })
                .doOnRequest(r -> {
                    System.out.printf("%d %s onRequest %d%n", flowId, flowDescription, r);
                })


                // data-flow

                .doOnNext(item -> {
                    System.out.printf("%d %s onNext: %s%n", flowId, flowDescription, item.toString());
                });
    }
}
