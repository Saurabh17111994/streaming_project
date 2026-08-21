package com.trading.common.schema.projection;

import com.trading.common.schema.position.FillEvent;
import com.trading.common.schema.position.PositionProjectorDriver;
import com.trading.common.schema.position.PositionSnapshot;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Differential-parity fixture generator (WP-4 step 3). NOT a real assertion test: it drives the
 * real {@link PositionProjectorDriver} oracle over a fixed fill scenario and prints the resulting
 * snapshots as a JSON document. The output is committed as the oracle fixture consumed by the Rust
 * {@code tests/differential_parity.rs} so the Rust emitter is proven to reproduce the JVM oracle
 * field-for-field (positive sequence + oversell negative).
 */
class DifferentialParityFixtureTest {

    private static final long NOW = 1000L;
    private static final String POSITION_ID = "pos-acc-1-1001-BUY-1";

    private static FillEvent fill(long seq, String side, long qty, long pricePaise) {
        return new FillEvent(POSITION_ID, "tc-1", "acc-1", 1001L, "CME", "wti", side,
                qty, pricePaise, "evt-" + seq, seq, NOW);
    }

    @Test
    void printOracleFixture() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("now", NOW);
        doc.put("position_id", POSITION_ID);

        // --- positive sequence ---
        PositionProjectorDriver pos = new PositionProjectorDriver();
        List<Map<String, Object>> steps = new ArrayList<>();
        long[][] ff = {{1, 10, 1000, 1}, {2, 5, 1100, 2}, {3, 8, 1050, 3}, {4, 7, 1060, 4}};
        for (long[] f : ff) {
            // {seq, qty, price, byte(for side index unused)}
            PositionProjectorDriver.FeedResult r = pos.feed(
                    fill(f[0], f[1] == 1 || f[0] <= 2 ? "BUY" : "SELL", f[1], f[2]), NOW);
            PositionSnapshot s = r.snapshot();
            Map<String, Object> step = new LinkedHashMap<>();
            step.put("seq", f[0]);
            step.put("outcome", r.outcome().name());
            step.put("state", s.state().name());
            step.put("open", s.openQuantity());
            step.put("closed", s.closedQuantity());
            step.put("avgEntry", s.averageEntryPaise());
            step.put("avgExit", s.averageExitPaise());
            step.put("sourceVersion", s.sourceVersion());
            step.put("sourceEventId", s.sourceEventId());
            steps.add(step);
        }
        doc.put("positive", steps);

        // --- oversell negative ---
        PositionProjectorDriver neg = new PositionProjectorDriver();
        neg.feed(fill(1, "BUY", 10, 1000), NOW);
        PositionProjectorDriver.FeedResult oversell = neg.feed(fill(2, "SELL", 20, 900), NOW);
        doc.put("oversell_outcome", oversell.outcome().name());
        doc.put("oversell_reason_has_overshoots", oversell.reason() != null
                && oversell.reason().contains("overshoots"));

        System.out.println("PARITY_FIXTURE_BEGIN");
        com.fasterxml.jackson.databind.ObjectMapper om =
                new com.fasterxml.jackson.databind.ObjectMapper();
        try {
            System.out.println(om.writeValueAsString(doc));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("PARITY_FIXTURE_END");
    }
}
