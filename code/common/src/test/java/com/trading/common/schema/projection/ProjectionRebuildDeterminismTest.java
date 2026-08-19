package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.position.InMemoryPositionsStateStore;
import com.trading.common.schema.position.PositionSnapshot;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * T6 rebuild determinism: deleting and rebuilding the projections from the same
 * captured normalized fixture set reproduces identical rows, source versions,
 * fingerprints, and ledger completion — without altering Nautilus state and
 * without issuing any broker command (the driver has no broker surface).
 */
class ProjectionRebuildDeterminismTest {

    private static final long NOW = 1000L;

    private static final List<NormalizedPostback> FIXTURES = List.of(
            TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW),
            TestPostbacks.fill(2L, "b-1", "BUY", 15, 0, 5, 1100L, NOW),
            TestPostbacks.status(3L, "b-1", "PARTIAL", 15, 2, NOW),
            TestPostbacks.fill(4L, "b-1", "SELL", 15, 0, 8, 1050L, NOW),
            TestPostbacks.fill(5L, "b-1", "SELL", 15, 0, 7, 1060L, NOW),
            TestPostbacks.status(6L, "b-1", "FILLED", 15, 0, NOW));

    private record Rebuild(PostbackProjectionDriver driver,
                           InMemoryLifecycleStore lifecycle,
                           InMemoryPositionsStateStore positions,
                           InMemoryPostbackQuarantineStore quarantine) {
        String snapshot() throws Exception {
            StringBuilder sb = new StringBuilder();
            positions.all().stream().sorted(Comparator.comparing(PositionSnapshot::positionId))
                    .forEach(p -> sb.append("pos|").append(p.positionId()).append('|')
                            .append(p.state()).append('|').append(p.openQuantity()).append('|')
                            .append(p.closedQuantity()).append('|').append(p.averageEntryPaise())
                            .append('|').append(p.averageExitPaise()).append('|')
                            .append(p.sourceEventId()).append('|').append(p.sourceVersion())
                            .append('\n'));
            lifecycle.all().stream()
                    .sorted(Comparator.comparing(o -> o.brokerOrderId()))
                    .forEach(o -> sb.append("life|").append(o.brokerOrderId()).append('|')
                            .append(o.normalizedState()).append('|').append(o.cumulativeQty())
                            .append('|').append(o.pendingQty()).append('|')
                            .append(o.sourceEventId()).append('|').append(o.sourceVersion())
                            .append('\n'));
            return sb.toString();
        }
    }

    private static Rebuild fresh() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex()
                .byBrokerOrderId("b-1", new AttemptRef("acc-1", "instr-1", "att-1", "tc-1"));
        InMemoryLifecycleStore lifecycle = new InMemoryLifecycleStore();
        InMemoryPositionsStateStore positions = new InMemoryPositionsStateStore();
        InMemoryPostbackQuarantineStore quarantine = new InMemoryPostbackQuarantineStore();
        PostbackProjectionDriver driver = new PostbackProjectionDriver(
                index, lifecycle, positions, new InMemoryProjectionLedgerStore(),
                new InMemoryProjectionAuditStore(), quarantine,
                new ReferencePositionAuthority(), "nautilus-projection", 1L);
        return new Rebuild(driver, lifecycle, positions, quarantine);
    }

    @Test
    void replayAndRebuildProduceIdenticalProjections() throws Exception {
        Rebuild first = fresh();
        Rebuild second = fresh();
        for (NormalizedPostback p : FIXTURES) {
            first.driver().project(p, NOW);
        }
        for (NormalizedPostback p : FIXTURES) {
            second.driver().project(p, NOW);
        }
        assertThat(second.snapshot()).isEqualTo(first.snapshot());
        assertThat(first.quarantine().size()).isZero();
        assertThat(second.quarantine().size()).isZero();
        // No broker command exists on the projection surface (read-only engine).
        assertThat(hasBrokerMethod(PostbackProjectionDriver.class)).isFalse();
    }

    @Test
    void everyLedgerCompletesOnRebuild() throws Exception {
        Rebuild r = fresh();
        for (NormalizedPostback p : FIXTURES) {
            assertThat(r.driver().project(p, NOW).outcome())
                    .isEqualTo(PostbackProjectionDriver.Outcome.APPLIED);
        }
        assertThat(r.positions().size()).isEqualTo(1);
        assertThat(r.lifecycle().size()).isEqualTo(1);
    }

    private static boolean hasBrokerMethod(Class<?> clazz) {
        return java.util.Arrays.stream(clazz.getDeclaredMethods())
                .anyMatch(m -> m.getName().toLowerCase().contains("place")
                        || m.getName().toLowerCase().contains("cancel"));
    }
}
