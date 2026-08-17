package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The window-level emit semantics that a {@code ProcessWindowFunctionTestHarness}
 * used to cover (removed in Flink 2.x — this harness drives the real
 * {@code WindowOperator} the {@code SignalJob} graph runs, built through the
 * same {@code WindowOperatorBuilder} path):
 *
 * <ul>
 *   <li><b>Item 1 — emitted-flag no-op.</b> A late event that re-triggers an
 *       already-emitted window calls {@code CandleEmitFunction.process} again,
 *       but the {@code emitted} window-state flag makes the re-trigger a no-op:
 *       exactly one candle row is ever emitted for a window (no correction
 *       row, no duplicate) — the dossier "no correction rows in MVP"
 *       (R-012) contract.</li>
 *   <li><b>Item 3 — SIG-HARNESS-002.</b> A late event arriving before the
 *       window's final boundary (within {@code ALLOWED_LATENESS_MS}) updates
 *       the in-flight accumulator and re-triggers emit (a no-op per the flag);
 *       an event arriving after the final boundary is discarded by the window
 *       operator to the {@code CandleLateDrop} side output — the permitted
 *       update/discard behavior.</li>
 * </ul>
 */
@DisplayName("candle window: emitted-flag no-op + late-before-final vs after-final (SIG-HARNESS-002)")
class CandleWindowEmitHarnessTest {

    /** 15000-aligned epoch anchor — window [T0, T0+15000) holds every offset used. */
    private static final long T0 = 1_749_999_990_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    @BeforeEach
    void setUp() throws Exception {
        // Production window semantics: 15000 ms tumbling, 5000 ms allowed lateness.
        harness = CandleWindowTestHarness.create(15_000L, 5_000L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    /** Emits a tick for token 2885 in the window starting at {@code T0}. */
    private void tick(long offsetMs, long price, String fingerprint) throws Exception {
        long eventTime = T0 + offsetMs;
        harness.processElement(
                new StreamRecord<>(TestRawRows.row(2885L, eventTime, fingerprint, "TRADE", price, 1),
                        eventTime));
    }

    private void advanceWatermarkPastWindowEnd() throws Exception {
        harness.processWatermark(new Watermark(T0 + 15_000L));
    }

    @Test
    @DisplayName("late re-trigger of an emitted window emits nothing (emitted-flag no-op)")
    void lateRetriggerEmitsNothing() throws Exception {
        // Four ticks inside the window.
        tick(1_000L, 100, "fp-a");
        tick(5_000L, 110, "fp-b");
        tick(9_000L, 105, "fp-c");
        tick(13_000L, 108, "fp-d");

        // Window closes: exactly one candle row.
        advanceWatermarkPastWindowEnd();
        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness),
                "a closed window emits exactly one candle row");

        // A late tick (event time still in the closed window, arriving within
        // allowed lateness) re-triggers the window operator → CandleEmitFunction
        // runs again with the emitted flag set → NO second row, no correction.
        tick(12_000L, 999, "fp-late");

        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness),
                "the emitted flag must make a re-trigger a no-op — no correction row");
    }

    @Test
    @DisplayName("a re-trigger keeps the original candle business fields (no correction)")
    void retriggerKeepsOriginalCandle() throws Exception {
        tick(1_000L, 100, "fp-a");
        tick(5_000L, 110, "fp-b");
        advanceWatermarkPastWindowEnd();
        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness));

        RowData original = candleRow();
        assertEquals(100L, original.getLong(CandleTableColumns.OPEN_PAISE));
        assertEquals(110L, original.getLong(CandleTableColumns.CLOSE_PAISE));

        // A late update at a different price must NOT rewrite the emitted candle.
        tick(7_000L, 999, "fp-late");
        RowData afterLate = candleRow();
        assertEquals(original.getLong(CandleTableColumns.CLOSE_PAISE),
                afterLate.getLong(CandleTableColumns.CLOSE_PAISE),
                "a late re-trigger must not correct the already-emitted candle");
        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness),
                "still exactly one candle row after the late re-trigger");
    }

    @Test
    @DisplayName("SIG-HARNESS-002: in-lateness update is a no-op; beyond-lateness is discarded to the late side output")
    void lateBeforeFinalUpdatesAndAfterFinalDiscards() throws Exception {
        tick(1_000L, 100, "fp-a");
        tick(5_000L, 110, "fp-b");
        advanceWatermarkPastWindowEnd();
        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness));
        assertEquals(0, lateSideOutputCount(),
                "no beyond-lateness drops yet");

        // Late-before-final: event time in the window, watermark past its end,
        // but within allowed lateness (5000 ms). Permitted behavior: the
        // accumulator re-triggers (no-op output), NOT a discard.
        tick(12_000L, 200, "fp-in-lateness");
        assertEquals(0, lateSideOutputCount(),
                "an in-lateness late event is NOT dropped to the late side output");
        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness),
                "and it does not emit a correction row");

        // Beyond-final: event time in the window, watermark past
        // window_end + allowed_lateness → the window operator discards it to
        // the CandleLateDrop side output (REQ-FC-006 observability path).
        harness.processWatermark(new Watermark(T0 + 15_000L + 5_000L + 1L));
        tick(11_000L, 300, "fp-beyond-lateness");
        assertEquals(1, lateSideOutputCount(),
                "a beyond-final event is discarded to the CandleLateDrop side output");
        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness),
                "main output still exactly one candle row");
    }

    private RowData candleRow() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (StreamRecord<?>) o)
                .map(sr -> (RowData) sr.getValue())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no candle row emitted"));
    }

    private int lateSideOutputCount() {
        var side = harness.getSideOutput(CandleLateDrop.OUTPUT);
        return side == null ? 0 : side.size();
    }
}
