package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * REQ-FC-006: raw ticks dropped by the 15 s candle window beyond
 * {@code window_end + ALLOWED_LATENESS_MS} are counted
 * ({@code compute.candles.late.dropped}) with the latest drop's attributes —
 * never silently discarded (dossier Required telemetry; DEC-038 bounded
 * cardinality — one attribute set, no per-key labels). The OTLP payload shape
 * (DELTA sum + the single bounded attribute set) is pinned by
 * {@code ComputeOtlpEmitterTest}; this test drives the counter OPERATOR.
 */
@DisplayName("CandleLateDrop counter (REQ-FC-006)")
class CandleLateDropTest {

    private static final long T0 = 1_700_000_000_000L;

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;
    private ComputeOtlpEmitter emitter;

    @BeforeEach
    void setUp() throws Exception {
        ComputeOtlpEmitter.resetDedupTelemetryForTest();
        emitter = new ComputeOtlpEmitter("localhost:4318");
        harness = ProcessFunctionTestHarnesses.forProcessFunction(
                new CandleLateDrop.CounterFunction(15_000L));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    @Test
    @DisplayName("each dropped tick is counted once and refreshes the latest-drop attributes")
    void countsDroppedTicksWithLatestAttributes() throws Exception {
        // Watermark far past the event time: the drop the window operator
        // would have performed is observable to the counter via the propagated
        // watermark (lateness = watermark - event_time).
        long watermark = T0 + 30_000L;
        harness.processWatermark(new Watermark(watermark));
        harness.processElement(new StreamRecord<>(
                TestRawRows.row(42L, T0, "fp-late", "TRADE", 100, 1), T0));
        harness.processElement(new StreamRecord<>(
                TestRawRows.row(7L, T0 + 1_000L, "fp-late-2", "TRADE", 101, 2), T0 + 1_000L));

        assertEquals(2L, emitter.drainCandleLateDropDelta(),
                "each dropped tick is counted exactly once");
        // A drained delta never re-fires.
        assertEquals(0L, emitter.drainCandleLateDropDelta());

        // The latest drop wins the single bounded attribute set.
        assertEquals(7L, ComputeOtlpEmitter.lateDropInstrumentForTest());
        long windowEnd = ((T0 + 1_000L) / 15_000L) * 15_000L + 15_000L;
        assertEquals(windowEnd, ComputeOtlpEmitter.lateDropWindowEndMsForTest());
        assertEquals(watermark - (T0 + 1_000L),
                ComputeOtlpEmitter.lateDropLatenessMsForTest());
        assertEquals(CandleLateDrop.REASON_BEYOND_ALLOWED_LATENESS,
                ComputeOtlpEmitter.lateDropReasonForTest());
    }

    @Test
    @DisplayName("no drops → zero-count window, no attributes recorded")
    void emptyWindowRecordsNothing() {
        assertEquals(0L, emitter.drainCandleLateDropDelta());
        assertEquals(-1L, ComputeOtlpEmitter.lateDropInstrumentForTest());
        assertEquals(null, ComputeOtlpEmitter.lateDropReasonForTest());
    }
}
