package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.AttemptStore.PrepareRequest;
import com.trading.common.schema.execution.AttemptStore.PrepareResult;
import com.trading.common.schema.execution.AttemptStore.TransitionResult;
import com.trading.common.schema.execution.GateStateStore.FenceResult;
import org.junit.jupiter.api.Test;

/**
 * WP-3 restart-refresh (offline): proves the hydration seams that the Fluss stores use to
 * re-derive a restarted process's authority from durable rows — without needing a live Fluss.
 * The durable cross-restart proof itself is the env-gated {@code FlussGateAttemptStoresIntegrationTest}.
 */
class InMemoryStoreHydrationTest {

    private static final long NOW = 1_700_000_000_000L;

    @Test
    void gateHydrateRebuildsFenceAndKeepsSequenceMonotonic() {
        InMemoryGateStateStore store = new InMemoryGateStateStore();
        // A durable row this (restarted) process has not witnessed: already fenced at token 5,
        // two-person approved, owner exec-1.
        GateRow durable = new GateRow("p1", "acc-1", GateState.ENABLED, 0L,
                null, null, "op-a", "op-b", "ev-1", "exec-1", 5L, NOW, NOW + 30_000L, null);
        store.hydrate(durable);

        GateRow recovered = store.read("p1");
        assertThat(recovered).isNotNull();
        assertThat(recovered.fenceToken()).isEqualTo(5L);
        assertThat(recovered.ownerInstanceId()).isEqualTo("exec-1");
        assertThat(recovered.approvalsComplete()).isTrue();

        // The next acquire must be strictly greater than the durable token (monotonic across
        // restart) — a re-seeded sequence, not a recycled one.
        FenceResult next = store.acquire("p1", "exec-1", 30_000L, NOW);
        assertThat(next.token()).isGreaterThan(5L);
    }

    @Test
    void attemptHydrateRebuildsDuplicateIndex() {
        InMemoryAttemptStore store = new InMemoryAttemptStore(() -> { });
        // A PREPARED attempt this (restarted) process has not witnessed.
        AttemptRecord rec = AttemptRecord.prepared("t1", "acc-1", "instr-1", "buy",
                "p1", "h1", "cref-1", 7L, 0L, NOW);
        store.hydrate(rec);

        // Re-preparing the same (instruction_id, request_hash) + deterministic attempt id returns
        // DUPLICATE, never a second CREATED — the crash-window exactly-once core.
        PrepareRequest req = new PrepareRequest("t1", "acc-1", "instr-1", "buy",
                "p1", "h1", "cref-1", 7L, 0L, NOW);
        PrepareResult r = store.prepare(req);
        assertThat(r.status()).isEqualTo(AttemptStore.Status.DUPLICATE);
        assertThat(r.record().phase()).isEqualTo("PREPARED");
        assertThat(store.attemptById("t1").phase()).isEqualTo("PREPARED");

        // A hydrated PREPARED attempt can transition normally (PREPARED -> SUBMITTING).
        TransitionResult t = store.transition("t1", 0L, "SUBMITTING");
        assertThat(t.outcome()).isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(t.record().phase()).isEqualTo("SUBMITTING");
        assertThat(t.record().phaseEpoch()).isEqualTo(1L);
    }
}
