package com.trading.execution.gateway;

/** Durable source-event dedup: process-local classification reconciled from a
 *  durable store so a replayed Execution_Intent is not handed off twice after a
 *  gateway restart (T2 durable attempt/index lookup). */
final class DurableIntentDispatcher {
    enum Verdict { FIRST, DUPLICATE, HASH_VIOLATION }
    private final IntentDedupStore store;
    private final IntentDeduplicator dedup = new IntentDeduplicator();

    DurableIntentDispatcher(IntentDedupStore store) throws Exception {
        this.store = store;
        store.hydrate().forEach(dedup::commit);
    }

    Verdict classify(String instructionId, String requestHash) {
        return switch (dedup.classify(instructionId, requestHash)) {
            case FIRST -> Verdict.FIRST;
            case DUPLICATE -> Verdict.DUPLICATE;
            case HASH_VIOLATION -> Verdict.HASH_VIOLATION;
        };
    }

    /** Called after a durably-successful handoff: update local + durable state. */
    void committed(String instructionId, String requestHash, Long logOffset) throws Exception {
        dedup.commit(instructionId, requestHash);
        store.record(instructionId, requestHash, logOffset);
    }
}
