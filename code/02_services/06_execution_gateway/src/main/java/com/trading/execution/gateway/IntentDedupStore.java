package com.trading.execution.gateway;

/** Durable store for source-event reprocessing dedup (Execution_Intent_Processed). */
public interface IntentDedupStore extends AutoCloseable {
    /** Reconcile every durably-processed source event: instructionId -> requestHash. */
    java.util.Map<String, String> hydrate() throws Exception;
    /** Durably record that {@code instructionId} (hash {@code requestHash}) was handed off. */
    void record(String instructionId, String requestHash, Long logOffset) throws Exception;
    @Override void close() throws Exception;
}
