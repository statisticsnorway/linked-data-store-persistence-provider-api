package no.ssb.lds.api.persistence.reactivex;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class RxGroupByTools {

    /**
     * Assumes that the upstream is pre-ordered by group, so that all upstream items belonging to the same group
     * will come immediately after one-another. This allows the grouped-flows to be immediately completed when an item
     * belonging to a different group is observed upstream.
     * <p>
     * The returned stream honors downstream back-pressure at the group-level, and will buffer all items in each group
     * until they are subscribed to and consumed. This allows more control over memory-usage than the built-in group-by
     * operations currently supported by RxJava.
     *
     * @param upstream         the upstream as a flow of upstream-items.
     * @param groupKeyFunction the function that will be applied to an upstream item to determine which group it
     *                         belongs to. This function must be idempotent (cannot have side effect as it might be
     *                         applied more than once).
     * @param <T>              the generic type of the upstream and downstream items.
     * @param <K>              the generic type of the group-key. This type must implement the equals and hashCode
     *                         methods so that the group-key of two different upstream-items (as provided by the
     *                         groupKeyFunction) that belong in the same group will be considered equal, otherwise (if
     *                         they belong in different groups will be considered not-equals.
     * @return a flow of groups where each group is a separate subflow of upstream items.
     */
    public static <T, K> Flowable<MyGroupedFlowable<T, K>> groupByOrdered(final Flowable<? extends T> upstream, final Function<? super T, ? extends K> groupKeyFunction) {
        final GroupedItemWrapper<T> groupCompleteItem = new GroupedItemWrapper<>((T) null);
        final GroupedItemWrapper<T> cancelledItem = new GroupedItemWrapper<>((T) null);

        final AtomicReference<K> groupKeyRef = new AtomicReference<>();
        final Map<K, BlockingQueue<GroupedItemWrapper<T>>> itemsByGroup = new LinkedHashMap<>();

        return upstream.filter(item -> {
            // Group by object determined by groupKeyFunction
            K groupKey = groupKeyFunction.apply(item);

            if (groupKeyRef.get() == null) {
                // very first upstream item, add item to new group
                groupKeyRef.set(groupKey);
                itemsByGroup.computeIfAbsent(groupKey, k -> new LinkedBlockingQueue<>()).add(new GroupedItemWrapper<>(item));
                return true;
            }
            if (groupKey.equals(groupKeyRef.get())) {
                // group already exists, add item to existing group
                itemsByGroup.computeIfAbsent(groupKey, k -> new LinkedBlockingQueue<>()).add(new GroupedItemWrapper<>(item));
                return false;
            }

            // new group, signal completion of previous group and add item to new group
            itemsByGroup.get(groupKeyRef.get()).add(groupCompleteItem);
            groupKeyRef.set(groupKey);
            itemsByGroup.computeIfAbsent(groupKey, k -> new LinkedBlockingQueue<>()).add(new GroupedItemWrapper<>(item));
            return true;

        }).doOnSubscribe(subscription -> {
            groupKeyRef.set(null);
            itemsByGroup.clear();

        }).doOnComplete(() -> {
            K groupKey = groupKeyRef.get();
            if (groupKey == null) {
                return;
            }
            itemsByGroup.get(groupKey).put(groupCompleteItem);

        }).doOnError(throwable -> {
            K groupKey = groupKeyRef.get();
            if (groupKey == null) {
                return;
            }
            itemsByGroup.get(groupKey).put(new GroupedItemWrapper<>(throwable));

        }).doOnCancel(() -> {
            K groupKey = groupKeyRef.get();
            if (groupKey == null) {
                return;
            }
            itemsByGroup.get(groupKey).put(cancelledItem);

        }).map(item -> {
            final K groupKey = groupKeyRef.get();
            final BlockingQueue<GroupedItemWrapper<T>> blockingQueue = itemsByGroup.get(groupKey);
            Flowable<T> innerGroupFlow = Flowable.generate(
                    () -> blockingQueue,
                    (queue, emitter) -> {
                        GroupedItemWrapper<T> wrapper = queue.take();
                        if (wrapper == groupCompleteItem) {
                            emitter.onComplete();
                            return;
                        }
                        if (wrapper == cancelledItem) {
                            emitter.onError(new RuntimeException("cancelled"));
                        }
                        if (wrapper.throwable != null) {
                            emitter.onError(wrapper.throwable);
                        }
                        emitter.onNext(wrapper.item);
                    },
                    queue -> queue.clear()
            );
            return new MyGroupedFlowable<>(groupKey, innerGroupFlow.subscribeOn(Schedulers.newThread()));
        });
    }

    public static class MyGroupedFlowable<T, K> {
        private final K groupKey;
        private final Flowable<T> flowable;

        private MyGroupedFlowable(K groupKey, Flowable<T> flowable) {
            this.groupKey = groupKey;
            this.flowable = flowable;
        }

        public K key() {
            return groupKey;
        }

        public Flowable<T> flowable() {
            return flowable;
        }
    }

    private static class GroupedItemWrapper<T> {
        final T item;
        final Throwable throwable;

        GroupedItemWrapper(T item) {
            this.item = item;
            this.throwable = null;
        }

        GroupedItemWrapper(Throwable t) {
            this.item = null;
            this.throwable = t;
        }
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
