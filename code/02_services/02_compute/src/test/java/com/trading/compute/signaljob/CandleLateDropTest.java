package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * {@code window_end + ALLOWED_LATENESS_MS} are counted into the
 * {@code compute.candles.late.dropped} MetricGroup counter — never silently
 * discarded (dossier Required telemetry). CHG-023 item 1 (2026-08-17): the
 * counter is exported by the native flink-metrics-otel reporter; the old
 * emitter's single bounded attribute set (latest drop's instrument/window/
 * lateness/reason) is NOT carried on the native metric path — the count
 * series + WARN-side observability remain. This test drives the counter
 * OPERATOR and asserts the MetricGroup counter's source value.
 */
@DisplayName("CandleLateDrop counter (REQ-FC-006)")
class CandleLateDropTest {

    private static final long T0 = 1_700_000_000_000L;

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;
    private CandleLateDrop.CounterFunction counterFunction;

    @BeforeEach
    void setUp() throws Exception {
        counterFunction = new CandleLateDrop.CounterFunction(15_000L);
        harness = ProcessFunctionTestHarnesses.forProcessFunction(counterFunction);
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
    @DisplayName("each dropped tick is counted exactly once into compute.candles.late.dropped")
    void countsDroppedTicksExactlyOnce() throws Exception {
        // Watermark far past the event time: the drop the window operator
        // would have performed is observable to the counter via the propagated
        // watermark (lateness = watermark - event_time).
        long watermark = T0 + 30_000L;
        harness.processWatermark(new Watermark(watermark));
        harness.processElement(new StreamRecord<>(
                TestRawRows.row(42L, T0, "fp-late", "TRADE", 100, 1), T0));
        harness.processElement(new StreamRecord<>(
                TestRawRows.row(7L, T0 + 1_000L, "fp-late-2", "TRADE", 101, 2), T0 + 1_000L));

        assertEquals(2L, counterFunction.droppedCountForTest(),
                "each dropped tick is counted exactly once into the MetricGroup counter");
    }

    @Test
    @DisplayName("no drops → counter stays at zero")
    void emptyWindowRecordsNothing() {
        assertEquals(0L, counterFunction.droppedCountForTest());
    }
}

