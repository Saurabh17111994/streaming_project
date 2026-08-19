package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.PositionState;
import com.trading.common.schema.position.FillEvent;
import com.trading.common.schema.position.InMemoryPositionsStateStore;
import com.trading.common.schema.position.PositionProjectorDriver;
import com.trading.common.schema.position.PositionSnapshot;
import org.junit.jupiter.api.Test;

/**
 * Differential parity (T6): the projection serialization path, driven by the
 * {@link ReferencePositionAuthority} (which routes position arithmetic to the
 * documented Java parity projector, standing in for Rust/Nautilus), must
 * reproduce exactly the reference projector's Positions rows (state, quantity
 * pair, weighted averages, source version/event). Negative cases (oversell)
 * must produce the same violation disposition on both paths. The oracle is
 * never invoked by runtime projection — only here by the test-time authority.
 */
class NautilusParityTest {

    private static final long NOW = 1000L;
    private static final String POSITION_ID = "pos-acc-1-1001-BUY-1";

    private record Pipeline(PostbackProjectionDriver driver,
                           InMemoryPositionsStateStore positions,
                           ReferencePositionAuthority authority) {}

    @Test
    void serializedRowsMatchReferenceProjectorAcrossValidSequence() throws Exception {
        ReferencePositionAuthority authority = new ReferencePositionAuthority();
        // Reference oracle: the authority computes positions with the parity
        // projector. Warm it through the same sequence this test drives below.
        authority.apply(TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW));
        authority.apply(TestPostbacks.fill(2L, "b-1", "BUY", 15, 0, 5, 1100L, NOW));
        authority.apply(TestPostbacks.fill(3L, "b-1", "SELL", 15, 0, 8, 1050L, NOW));
        authority.apply(TestPostbacks.fill(4L, "b-1", "SELL", 15, 0, 7, 1060L, NOW));
        PositionProjectorDriver oracle = authority.oracle();
        PositionSnapshot oracleRow = oracle.snapshot(POSITION_ID);
        assertThat(oracleRow).isNotNull();
        assertThat(oracleRow.state()).isEqualTo(PositionState.CLOSED);
        assertThat(oracleRow.openQuantity()).isEqualTo(15);
        assertThat(oracleRow.closedQuantity()).isEqualTo(15);

        // Drive the projection pipeline with the SAME sequence from empty stores.
        Pipeline pipe = pipeline();
        assertThat(apply(pipe, 1L, "BUY", 10, 1000L))
                .isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        assertThat(apply(pipe, 2L, "BUY", 5, 1100L))
                .isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        assertThat(apply(pipe, 3L, "SELL", 8, 1050L))
                .isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        assertThat(apply(pipe, 4L, "SELL", 7, 1060L))
                .isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);

        PositionSnapshot projected = pipe.positions().lookup(POSITION_ID);
        assertThat(projected).isNotNull();
        assertThat(projected.state()).isEqualTo(oracleRow.state());
        assertThat(projected.openQuantity()).isEqualTo(oracleRow.openQuantity());
        assertThat(projected.closedQuantity()).isEqualTo(oracleRow.closedQuantity());
        assertThat(projected.averageEntryPaise()).isEqualTo(oracleRow.averageEntryPaise());
        assertThat(projected.averageExitPaise()).isEqualTo(oracleRow.averageExitPaise());
        assertThat(projected.sourceVersion()).isEqualTo(oracleRow.sourceVersion());
        assertThat(projected.sourceEventId()).isEqualTo(oracleRow.sourceEventId());
    }

    @Test
    void oversellDispositionAgreesBetweenProjectionAndReference() {
        // Reference oracle rejects an oversell as VIOLATION.
        PositionProjectorDriver oracle = new PositionProjectorDriver();
        oracle.feed(fill(1L, "BUY", 10, 1000L), NOW);
        PositionProjectorDriver.FeedResult oversell = oracle.feed(fill(2L, "SELL", 20, 900L), NOW);
        assertThat(oversell.outcome()).isEqualTo(PositionProjectorDriver.FeedOutcome.VIOLATION);

        // Projection path rejects the same oversell (quarantine + halt).
        Pipeline pipe = pipeline();
        assertThat(apply(pipe, 1L, "BUY", 10, 1000L))
                .isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        PostbackProjectionDriver.ProjectionResult r = pipe.driver().project(
                TestPostbacks.fill(2L, "b-1", "SELL", 10, 0, 20, 900L, NOW + 1), NOW + 1);
        assertThat(r.outcome()).isEqualTo(PostbackProjectionDriver.Outcome.QUARANTINED);
        assertThat(pipe.driver().haltedScopeIds()).contains("acc-1");
    }

    private static FillEvent fill(long seq, String side, long qty, long price) {
        return new FillEvent(POSITION_ID, "tc-1", "acc-1", 1001L, "CME", "wti", side,
                qty, price, "evt-" + seq, seq, NOW);
    }

    private static PostbackProjectionDriver.Outcome apply(Pipeline pipe, long seq,
            String side, long cumulativeDelta, long price) {
        return pipe.driver().project(
                TestPostbacks.fill(seq, "b-1", side, cumulativeDeltaBetween(seq, side),
                        0, qtyDelta(seq, side), price, NOW), NOW).outcome();
    }

    private static Pipeline pipeline() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex()
                .byBrokerOrderId("b-1", new AttemptRef("acc-1", "instr-1", "att-1", "tc-1"));
        InMemoryPositionsStateStore positions = new InMemoryPositionsStateStore();
        ReferencePositionAuthority authority = new ReferencePositionAuthority();
        PostbackProjectionDriver driver = new PostbackProjectionDriver(
                index, new InMemoryLifecycleStore(), positions,
                new InMemoryProjectionLedgerStore(), new InMemoryProjectionAuditStore(),
                new InMemoryPostbackQuarantineStore(), authority,
                "nautilus-projection", 1L);
        return new Pipeline(driver, positions, authority);
    }

    private static long cumulativeDeltaBetween(long seq, String side) {
        return switch ((int) seq) {
            case 1 -> 10;
            case 2 -> 15;
            case 3 -> 15;
            case 4 -> 15;
            default -> throw new IllegalStateException();
        };
    }

    private static long qtyDelta(long seq, String side) {
        return switch ((int) seq) {
            case 1 -> 10;
            case 2 -> 5;
            case 3 -> 8;
            case 4 -> 7;
            default -> throw new IllegalStateException();
        };
    }
}
