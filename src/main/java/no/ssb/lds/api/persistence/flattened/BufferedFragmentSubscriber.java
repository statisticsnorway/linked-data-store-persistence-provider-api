package no.ssb.lds.api.persistence.flattened;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class BufferedFragmentSubscriber implements Flow.Subscriber<Fragment> {

    private final CompletableFuture<FlattenedDocumentIterator> result;
    private final int fragmentValueCapacityBytes;
    private final String path;
    private final String value;
    private final int limit;

    private final AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();
    private final AtomicBoolean limitedMatchesRef = new AtomicBoolean(false);
    private final AtomicReference<DocumentKey> documentKeyRef = new AtomicReference<>();
    private final Map<String, List<Fragment>> fragmentsByPath = new TreeMap<>();
    private final List<FlattenedDocument> documents = new ArrayList<>();

    BufferedFragmentSubscriber(CompletableFuture<FlattenedDocumentIterator> result, int fragmentValueCapacityBytes, String path, String value, int limit) {
        this.result = result;
        this.fragmentValueCapacityBytes = fragmentValueCapacityBytes;
        this.path = path;
        this.value = value;
        this.limit = limit;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(2);
    }

    @Override
    public void onNext(Fragment fragment) {
        if (fragment.isStreamingControl()) {
            limitedMatchesRef.set(fragment.isLimited());
            return;
        }

        if (documents.size() >= limit) {
            // document limit reached
            subscriptionRef.get().cancel();
            return;
        }

        DocumentKey fragmentDocumentKey = DocumentKey.from(fragment);

        documentKeyRef.compareAndSet(null, fragmentDocumentKey);

        if (documentKeyRef.get().equals(fragmentDocumentKey)) {
            fragmentsByPath.computeIfAbsent(fragment.path(), path -> new ArrayList<>()).add(fragment);
        } else {
            addPendingDocumentAndResetMap();
            fragmentsByPath.computeIfAbsent(fragment.path(), path -> new ArrayList<>()).add(fragment);
            documentKeyRef.set(fragmentDocumentKey);
        }

        subscriptionRef.get().request(1);
    }

    private void addPendingDocumentAndResetMap() {
        if (!fragmentsByPath.isEmpty()) {
            FlattenedDocument document = FlattenedDocument.decodeDocument(documentKeyRef.get(), fragmentsByPath, fragmentValueCapacityBytes);
            if (path != null) {
                FlattenedDocumentLeafNode leafNode = document.leafNodesByPath().get(path);
                if (leafNode != null) {
                    if (value == null) {
                        if (leafNode.value() == null) {
                            documents.add(document);
                        }
                    } else if (value.equals(leafNode.value())) {
                        documents.add(document);
                    } else {
                        nop(); // false positive index match, document discarded
                    }
                } else {
                    nop(); // Index/primary mismatch or a bug! Got document that does not match expected path!
                }
            } else {
                documents.add(document);
            }
            fragmentsByPath.clear();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        addPendingDocumentAndResetMap();
        result.complete(new FlattenedDocumentIterator(documents));
    }

    private void nop() {
    }
}
