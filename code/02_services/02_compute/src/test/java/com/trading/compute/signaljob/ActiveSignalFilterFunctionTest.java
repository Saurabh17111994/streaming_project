package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Max-one-active per instrument — STRICT indefinite (user rule 2026-08-18,
 * Option B): if instrument already has an ACTIVE candidate, drop new one
 * — wait till explicit CLOSED (no TTL). Until Position_State feedback is
 * wired (#17), the block is indefinite and survives processing-time advance.
 */
class ActiveSignalFilterFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHarness() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new ActiveSignalFilterFunction(config),
                row -> row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private static RowData candidate(long token, String id, long ts) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(id));
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("TEST"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString("simple-breakout"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString("1.0.0"));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString("breakout-20-bullish-trend"));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, ts);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, ts);
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString("ENTRY"));
        row.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString("BUY"));
        row.setField(SignalCandidatesTableColumns.QUANTITY, 1L);
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE, StringData.fromString("MARKET"));
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION, StringData.fromString("2"));
        return row;
    }

    private static List<RowData> emitted(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .toList();
    }

    @Test
    void firstSignalPasses() throws Exception {
        openHarness();
        harness.processElement(candidate(1L, "id-1", 1000L), 1000L);
        assertEquals(1, emitted(harness).size());
    }

    @Test
    void secondSignalSameInstrumentDroppedWhileActive() throws Exception {
        openHarness();
        harness.processElement(candidate(1L, "id-1", 1000L), 1000L);
        harness.processElement(candidate(1L, "id-2", 2000L), 2000L);
        assertEquals(1, emitted(harness).size(), "second signal must be dropped while ACTIVE");
        // different instrument must pass
        harness.processElement(candidate(2L, "id-3", 3000L), 3000L);
        assertEquals(2, emitted(harness).size(), "different instrument must pass");
    }

    @Test
    void indefiniteBlockSurvivesProcessingTimeAdvance() throws Exception {
        openHarness();
        harness.processElement(candidate(1L, "id-1", 1000L), 1000L);
        assertEquals(1, emitted(harness).size());
        // second before "expiry" — would have been TTL 5s, but now indefinite — still dropped
        harness.processElement(candidate(1L, "id-2", 2000L), 2000L);
        assertEquals(1, emitted(harness).size());

        // advance processing time far past the old 5s TTL (6000, 10000, 1h) — still blocked
        harness.setProcessingTime(6000L);
        harness.processElement(candidate(1L, "id-3", 7000L), 7000L);
        assertEquals(1, emitted(harness).size(), "without CLOSED, block is indefinite — no TTL");

        harness.setProcessingTime(3_600_000L);
        harness.processElement(candidate(1L, "id-4", 3_601_000L), 3_601_000L);
        assertEquals(1, emitted(harness).size(), "still blocked after 1h — wait for CLOSED");
    }

    @Test
    void remainsBlockedAcrossManyWindows() throws Exception {
        openHarness();
        harness.processElement(candidate(5L, "id-a", 100L), 100L);
        harness.processElement(candidate(5L, "id-b", 200L), 200L);
        assertEquals(1, emitted(harness).size());
        harness.setProcessingTime(10_000L);
        harness.processElement(candidate(5L, "id-c", 11_000L), 11_000L);
        assertEquals(1, emitted(harness).size(), "indefinite — no auto-free");
    }
}
