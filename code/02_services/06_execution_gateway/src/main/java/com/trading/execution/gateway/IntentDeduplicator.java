package com.trading.execution.gateway;

import java.util.HashMap;
import java.util.Map;

/** In-process guard; the durable attempt/index tables remain the recovery authority. */
public final class IntentDeduplicator {
    public enum Outcome { FIRST, DUPLICATE, HASH_VIOLATION }
    private final Map<String, String> hashes = new HashMap<>();

    public synchronized Outcome classify(String instructionId, String requestHash) {
        String old = hashes.get(instructionId);
        if (old == null) return Outcome.FIRST;
        return old.equals(requestHash) ? Outcome.DUPLICATE : Outcome.HASH_VIOLATION;
    }
    public synchronized void commit(String instructionId, String requestHash) {
        hashes.putIfAbsent(instructionId, requestHash);
    }
}
