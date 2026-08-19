package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.OrderLifecycleState;
import org.junit.jupiter.api.Test;

class OrderLifecycleProjectorTest {

    private static final AttemptRef REF = new AttemptRef("acc-1", "instr-1", "att-1", "tc-1");
    private static final long NOW = 1000L;

    @Test
    void appliesCleanFillProgressingLifecycle() {
        NormalizedPostback p1 = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        OrderLifecycleProjector.LifecycleResult r1 =
                OrderLifecycleProjector.apply(null, p1, REF, NOW);
        assertThat(r1.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.APPLIED);
        assertThat(r1.snapshot().normalizedState()).isEqualTo(OrderLifecycleState.PARTIAL);
        assertThat(r1.snapshot().sourceVersion()).isEqualTo(1L);
        assertThat(r1.snapshot().averageFillPricePaise()).isEqualTo(1000L);

        NormalizedPostback p2 = TestPostbacks.status(2L, "b-1", "FILLED",
                r1.snapshot().cumulativeQty(), 0, NOW);
        OrderLifecycleProjector.LifecycleResult r2 =
                OrderLifecycleProjector.apply(r1.snapshot(), p2, REF, NOW);
        assertThat(r2.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.APPLIED);
        assertThat(r2.snapshot().normalizedState()).isEqualTo(OrderLifecycleState.FILLED);
    }

    @Test
    void exactDuplicateIsNoOp() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        OrderLifecycleProjector.LifecycleResult first =
                OrderLifecycleProjector.apply(null, p, REF, NOW);
        assertThat(first.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.APPLIED);
        OrderLifecycleProjector.LifecycleResult second =
                OrderLifecycleProjector.apply(first.snapshot(), p, REF, NOW);
        assertThat(second.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.DUPLICATE);
    }

    @Test
    void olderSourceVersionIsStale() {
        NormalizedPostback p2 = TestPostbacks.fill(2L, "b-1", "BUY", 20, 0, 10, 1000L, NOW);
        OrderLifecycleProjector.LifecycleResult newer =
                OrderLifecycleProjector.apply(null, p2, REF, NOW);
        assertThat(newer.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.APPLIED);
        NormalizedPostback p1 = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        OrderLifecycleProjector.LifecycleResult stale =
                OrderLifecycleProjector.apply(newer.snapshot(), p1, REF, NOW);
        assertThat(stale.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.STALE);
    }

    @Test
    void equalVersionDifferentContentIsConflict() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        OrderLifecycleProjector.LifecycleResult first =
                OrderLifecycleProjector.apply(null, p, REF, NOW);
        NormalizedPostback conflict = new NormalizedPostback(
                p.postbackEventId(), "evt-other", p.sourceSequence(), "fp-c",
                p.fingerprintVersion(), p.brokerOrderId(), p.echoedClientOrderRef(),
                p.accountScopeId(), p.instrumentToken(), p.exchange(), p.symbol(), p.side(),
                p.orderStatus(), p.cumulativeQty(), p.pendingQty(), p.fillQty(),
                p.fillPricePaise(), p.eventTimeMs(), p.receiveTimeMs(), p.mappingVersion(),
                p.originalPayloadHash(), p.tradeContextId());
        OrderLifecycleProjector.LifecycleResult r =
                OrderLifecycleProjector.apply(first.snapshot(), conflict, REF, NOW);
        assertThat(r.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.CONFLICT);
        assertThat(r.reason()).isEqualTo(QuarantineReason.LIFECYCLE_CONFLICT);
    }

    @Test
    void terminalRegressionIsRejected() {
        NormalizedPostback filled = TestPostbacks.status(1L, "b-1", "FILLED", 10, 0, NOW);
        OrderLifecycleProjector.LifecycleResult r1 =
                OrderLifecycleProjector.apply(null, filled, REF, NOW);
        assertThat(r1.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.APPLIED);

        NormalizedPostback regress = TestPostbacks.status(2L, "b-1", "PENDING", 10, 10, NOW);
        OrderLifecycleProjector.LifecycleResult r2 =
                OrderLifecycleProjector.apply(r1.snapshot(), regress, REF, NOW);
        assertThat(r2.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.REGRESSION);
        assertThat(r2.reason()).isEqualTo(QuarantineReason.TERMINAL_REGRESSION);
    }

    @Test
    void impossibleQuantityQuarantines() {
        NormalizedPostback p = TestPostbacks.status(1L, "b-1", "PENDING", 5, 10, NOW); // pending>cumulative
        OrderLifecycleProjector.LifecycleResult r =
                OrderLifecycleProjector.apply(null, p, REF, NOW);
        assertThat(r.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.UNKNOWN);
        assertThat(r.reason()).isEqualTo(QuarantineReason.IMPOSSIBLE_QUANTITY);
    }

    @Test
    void unknownStatusQuarantines() {
        NormalizedPostback p = TestPostbacks.status(1L, "b-1", "NOT_A_STATE", 10, 0, NOW);
        OrderLifecycleProjector.LifecycleResult r =
                OrderLifecycleProjector.apply(null, p, REF, NOW);
        assertThat(r.outcome()).isEqualTo(OrderLifecycleProjector.Outcome.UNKNOWN);
        assertThat(r.reason()).isEqualTo(QuarantineReason.UNKNOWN_STATUS);
    }
}
