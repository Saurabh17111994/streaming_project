package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Streaming-3000 T6 wiring through the REAL window operator
 * ({@link CandleWindowTestHarness}): a candle that fails an invariant is
 * quarantined on the {@link CandleQuarantine#OUTPUT} side output and never
 * reaches the main output (so it cannot reach the KV sink or signal
 * detection); a clean candle still emits exactly once and nothing is
 * quarantined. The violating candle is produced the only way valid
 * aggregation can: {@code long} volume overflow (two TRADE ticks of
 * {@code Long.MAX_VALUE} qty sum negative) — checks 1/2/5 cannot fail from
 * valid aggregation by construction, so they are covered by
 * {@link CandleInvariantCheckTest}.
 */
@DisplayName("candle window: OHLC-invariant violation → quarantine, never emitted (T6)")
class CandleInvariantQuarantineEmitTest {

    /** 15000-aligned epoch anchor — window [T0, T0+15000) holds every offset used. */
    private static final long T0 = 1_749_999_990_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = CandleWindowTestHarness.create(15_000L, 5_000L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void tick(long offsetMs, long price, long qty, String fingerprint) throws Exception {
        long eventTime = T0 + offsetMs;
        harness.processElement(
                new StreamRecord<>(TestRawRows.row(2885L, eventTime, fingerprint, "TRADE", price, qty),
                        eventTime));
    }

    private void advanceWatermarkPastWindowEnd() throws Exception {
        harness.processWatermark(new Watermark(T0 + 15_000L));
    }

    @SuppressWarnings("unchecked")
    private List<RowData> quarantineRows() {
        ConcurrentLinkedQueue<StreamRecord<RowData>> side =
                harness.getSideOutput(CandleQuarantine.OUTPUT);
        return side == null ? List.of()
                : side.stream()
                        .map(sr -> sr.getValue())
                        .map(r -> (RowData) r)
                        .toList();
    }

    @Test
    @DisplayName("an invariant-violating candle is quarantined and NOT emitted on the main output")
    void invalidCandleIsQuarantinedNotEmitted() throws Exception {
        // Two TRADE ticks of Long.MAX_VALUE qty: volume overflows to a negative
        // long → check 3 (VOLUME) fires. Prices are consistent (high >= max,
        // low <= min), so VOLUME is the one and only violation.
        tick(1_000L, 100, Long.MAX_VALUE, "fp-a");
        tick(5_000L, 110, Long.MAX_VALUE, "fp-b");
        advanceWatermarkPastWindowEnd();

        assertEquals(0L, CandleWindowTestHarness.candleRowCount(harness),
                "the invalid candle must NOT be emitted to the main output "
                        + "(no KV sink write, no signal detection input)");
        List<RowData> quarantined = quarantineRows();
        assertEquals(1, quarantined.size(),
                "exactly one quarantine row per violated window");
        RowData row = quarantined.get(0);
        assertEquals("INVALID_CANDLE_VOLUME",
                row.getString(CandleQuarantineColumns.REASON).toString());
        assertEquals(2885L, row.getLong(CandleQuarantineColumns.INSTRUMENT_TOKEN));
        assertTrue(row.getString(CandleQuarantineColumns.QUARANTINE_ID).toString()
                        .startsWith("compute-candle-"),
                "quarantine_id follows the compute-candle-UUID scheme");
        assertEquals(CandleQuarantineColumns.SCHEMA_VERSION,
                row.getString(CandleQuarantineColumns.SCHEMA_VERSION_INDEX).toString());
        assertTrue(row.getString(CandleQuarantineColumns.DETAIL).toString()
                        .contains("volume=-2"),
                "detail preserves the offending overflowed volume");
    }

    @Test
    @DisplayName("a valid candle still emits exactly once; nothing is quarantined")
    void validCandleEmitsNormally() throws Exception {
        tick(1_000L, 100, 10, "fp-a");
        tick(5_000L, 110, 20, "fp-b");
        advanceWatermarkPastWindowEnd();

        assertEquals(1L, CandleWindowTestHarness.candleRowCount(harness),
                "a clean candle emits exactly once");
        assertEquals(0, quarantineRows().size(),
                "no quarantine rows for a clean candle");
    }

    @Test
    @DisplayName("a late re-trigger of an already-quarantined window does not double-quarantine")
    void lateRetriggerDoesNotDoubleQuarantine() throws Exception {
        tick(1_000L, 100, Long.MAX_VALUE, "fp-a");
        tick(5_000L, 110, Long.MAX_VALUE, "fp-b");
        advanceWatermarkPastWindowEnd();
        assertEquals(1, quarantineRows().size());

        // Late event within allowed lateness re-triggers the window: the emitted
        // flag is set for the quarantined window, so no second quarantine row.
        tick(12_000L, 120, Long.MAX_VALUE, "fp-late");
        assertEquals(1, quarantineRows().size(),
                "a re-trigger must not re-quarantine the same window");
        assertEquals(0L, CandleWindowTestHarness.candleRowCount(harness),
                "and the late re-trigger still never emits the invalid candle");
    }
}
