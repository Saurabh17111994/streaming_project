package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Offline proof that IntentReader source-event dedup survives a restart: the
 *  process-local duplicate guard is reconciled from a durable store, so a
 *  replayed (instruction_id, request_hash) is never handed off twice. */
class DurableIntentDispatcherTest {
    /** In-memory stand-in for the durable Execution_Intent_Processed KV store. */
    static final class FakeDedupStore implements IntentDedupStore {
        final Map<String, String> durable = new HashMap<>();
        final List<String> recorded = new ArrayList<>();
        FakeDedupStore() {}
        FakeDedupStore(String id, String hash) { durable.put(id, hash); }
        @Override public Map<String, String> hydrate() { return new HashMap<>(durable); }
        @Override public void record(String instructionId, String requestHash, Long logOffset) {
            durable.put(instructionId, requestHash);
            recorded.add(instructionId);
        }
        @Override public void close() {}
    }

    @Test void firstHandoffBecomesDurableDuplicate() throws Exception {
        FakeDedupStore store = new FakeDedupStore();
        DurableIntentDispatcher d = new DurableIntentDispatcher(store);
        assertThat(d.classify("instr-1", "hash-A")).isEqualTo(
                DurableIntentDispatcher.Verdict.FIRST);
        d.committed("instr-1", "hash-A", 7L);
        // Durable + local now both know about it.
        assertThat(store.durable).containsEntry("instr-1", "hash-A");
        assertThat(store.recorded).containsExactly("instr-1");
        assertThat(d.classify("instr-1", "hash-A")).isEqualTo(
                DurableIntentDispatcher.Verdict.DUPLICATE);
    }

    @Test void restartWithWarmDurableSetSkipsDuplicate() throws Exception {
        // Simulates a gateway restart after a crash: the durable store still has
        // the handoff, but the in-memory guard starts empty again. A fresh
        // dispatcher hydrates the durable set and must NOT hand off a second time.
        FakeDedupStore store = new FakeDedupStore("instr-1", "hash-A");
        DurableIntentDispatcher afterRestart = new DurableIntentDispatcher(store);
        assertThat(afterRestart.classify("instr-1", "hash-A")).isEqualTo(
                DurableIntentDispatcher.Verdict.DUPLICATE);
    }

    @Test void changedHashAfterRestartIsHashViolation() throws Exception {
        FakeDedupStore store = new FakeDedupStore("instr-1", "hash-A");
        DurableIntentDispatcher afterRestart = new DurableIntentDispatcher(store);
        assertThat(afterRestart.classify("instr-1", "hash-CHANGED")).isEqualTo(
                DurableIntentDispatcher.Verdict.HASH_VIOLATION);
    }
}
