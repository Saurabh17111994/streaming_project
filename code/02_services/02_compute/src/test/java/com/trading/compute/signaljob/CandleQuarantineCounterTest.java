package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Streaming-3000 T6: the quarantine counter operator — each invalid candle is
 * counted exactly once into {@code compute.candles.invalid.total} plus its
 * per-reason {@code compute.candles.invalid.<reason>} MetricGroup counter
 * (the plan's {@code compute.candles.invalid.*} telemetry), and the row is
 * forwarded unchanged to the quarantine sink. Mirrors the
 * {@code CandleLateDrop.CounterFunction} test shape.
 */
@DisplayName("CandleQuarantine counter (compute.candles.invalid.*)")
class CandleQuarantineCounterTest {

    private OneInputStreamOperatorTestHarness<RowData, RowData> harness;
    private CandleQuarantine.CounterFunction counterFunction;

    @BeforeEach
    void setUp() throws Exception {
        counterFunction = new CandleQuarantine.CounterFunction();
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

    private static RowData quarantineRow(String reason, String id) {
        GenericRowData row = new GenericRowData(CandleQuarantineColumns.FIELD_COUNT);
        row.setField(CandleQuarantineColumns.QUARANTINE_ID, StringData.fromString(id));
        row.setField(CandleQuarantineColumns.REASON, StringData.fromString(reason));
        row.setField(CandleQuarantineColumns.DETAIL,
                StringData.fromString("detail-of-" + id));
        return row;
    }

    @Test
    @DisplayName("each invalid candle counts once into total and its per-reason counter")
    void countsPerReasonAndTotal() throws Exception {
        harness.processElement(new StreamRecord<>(
                quarantineRow("INVALID_CANDLE_HIGH", "q-1"), 0L));
        harness.processElement(new StreamRecord<>(
                quarantineRow("INVALID_CANDLE_HIGH", "q-2"), 0L));
        harness.processElement(new StreamRecord<>(
                quarantineRow("INVALID_CANDLE_LOW", "q-3"), 0L));

        assertEquals(3L, counterFunction.invalidTotalCountForTest(),
                "total counts every invalid candle exactly once");
        assertEquals(2L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.HIGH));
        assertEquals(1L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.LOW));
        assertEquals(0L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.VOLUME));
        assertEquals(0L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.WINDOW_SPAN));
        assertEquals(0L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.TICK_COUNT));
    }

    @Test
    @DisplayName("an unknown reason code still counts into total but not a per-reason bucket")
    void unknownReasonCountsTotalOnly() throws Exception {
        harness.processElement(new StreamRecord<>(
                quarantineRow("NOT_A_CANDLE_REASON", "q-x"), 0L));
        assertEquals(1L, counterFunction.invalidTotalCountForTest());
        assertEquals(0L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.HIGH),
                "unknown codes must not land in a real per-reason bucket");
    }

    @Test
    @DisplayName("the row is forwarded unchanged to the quarantine sink")
    void forwardsRowUnchanged() throws Exception {
        RowData row = quarantineRow("INVALID_CANDLE_VOLUME", "q-fwd");
        harness.processElement(new StreamRecord<>(row, 0L));
        assertEquals(1, harness.getOutput().size(),
                "the counter forwards exactly one row downstream");
        RowData out = (RowData) harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (StreamRecord<?>) o)
                .map(sr -> (RowData) sr.getValue())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no forwarded row"));
        assertEquals("q-fwd",
                out.getString(CandleQuarantineColumns.QUARANTINE_ID).toString());
        assertEquals("INVALID_CANDLE_VOLUME",
                out.getString(CandleQuarantineColumns.REASON).toString());
    }

    @Test
    @DisplayName("no invalid candles → all counters stay at zero")
    void emptyInputRecordsNothing() {
        assertEquals(0L, counterFunction.invalidTotalCountForTest());
        assertEquals(0L, counterFunction.invalidCountForTest(CandleInvariantCheck.Reason.HIGH));
    }
}
