package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link FingerprintDedupFunction} driven through the Flink 2.2.1 operator
 * harness (KeyedOneInputStreamOperatorTestHarness — no cluster). Covers the
 * dedup half of SIG-UNIT-008/009 (state content/compactness + expiry deletion):
 * first occurrence passes, duplicate within TTL is dropped, state holds exactly
 * the compact (fingerprint → first_seen/expiry) entries plus the expiry index,
 * and the expiry timer deletes entries at watermark ≥ first_seen + TTL — never
 * early, with a re-arriving fingerprint after expiry eligible again.
 */
class FingerprintDedupFunctionTest {

    private static final long TTL_MS = 300_000L;
    private static final long T0 = 1_700_000_000_000L;

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
                new FingerprintDedupFunction(config),
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    /** Same 5-key baseline as SignalJobConfigTest; tuning keys take defaults. */
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

    private static long emittedCount(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream().filter(o -> o instanceof StreamRecord).count();
    }

    private void process(GenericRowData row) throws Exception {
        harness.processElement(row, row.getLong(RawTableColumns.EVENT_TIME));
    }

    @Test
    void firstOccurrencePassesDuplicateWithinTtlDropped() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));

        assertEquals(1, emittedCount(harness), "duplicate within TTL must be dropped");
        // State layout contract (SIG-UNIT-008): per active key exactly two
        // state rows — the fingerprint-dedup map and the expiry-index map.
        // Fingerprint count adds map ENTRIES, never new rows: no raw payload,
        // decoded field, or event object is stored per fingerprint.
        assertEquals(2, harness.numKeyedStateEntries(), "one dedup row + one expiry-index row per active key");
        assertEquals(1, harness.numEventTimeTimers(), "one expiry timer per live fingerprint");
    }

    @Test
    void expiredFingerprintReAdmittedAfterExpiryTimer() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));
        assertEquals(1, emittedCount(harness));

        // Timer registered at first_seen + TTL fires when the watermark reaches it.
        harness.processWatermark(T0 + TTL_MS);
        assertEquals(0, harness.numKeyedStateEntries(), "expiry timer must delete the entry (absent after timer runs)");
        assertEquals(0, harness.numEventTimeTimers());

        // Re-arriving fingerprint after expiry is eligible again (SIG-UNIT-009).
        process(TestRawRows.row(1L, T0 + TTL_MS + 10_000L, "fp-1", "TRADE", 200, 5));
        assertEquals(2, emittedCount(harness));
    }

    @Test
    void entryNeverDeletedBeforeWatermarkReachesExpiry() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));

        // Watermark still below nominal expiry: entry must exist and still dedupe.
        harness.processWatermark(T0 + TTL_MS - 1L);
        assertEquals(2, harness.numKeyedStateEntries(), "never deleted early");
        process(TestRawRows.row(1L, T0 + TTL_MS - 1L, "fp-1", "TRADE", 101, 2));
        assertEquals(1, emittedCount(harness), "duplicate still dropped while unexpired");
    }

    @Test
    void stateKeyScopedByInstrumentToken() throws Exception {
        openHarness();
        // Same fingerprint on a different instrument is a different state key
        // (version | scope | fingerprint) — both must pass.
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(2L, T0, "fp-1", "TRADE", 100, 1));

        assertEquals(2, emittedCount(harness));
        // Two active keys → 2 rows × 2 state maps = 4.
        assertEquals(4, harness.numKeyedStateEntries(), "2 dedup + 2 expiry-index rows");
    }

    @Test
    void distinctFingerprintsSameInstrumentAllPass() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-2", "TRADE", 101, 1));

        assertEquals(2, emittedCount(harness));
        // One active key: two fingerprints add map entries, NOT state rows —
        // still exactly 2 rows (compactness: no per-fingerprint storage).
        assertEquals(2, harness.numKeyedStateEntries(), "fingerprint count never grows state rows");
        assertEquals(2, harness.numEventTimeTimers(), "timer per fingerprint, row per key");
    }

    @Test
    void oneTimerAtSharedExpiryClearsAllEntries() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0, "fp-2", "TRADE", 101, 1));
        assertEquals(2, harness.numKeyedStateEntries(), "one key, two fingerprints → two rows");
        // Same event time → same expiry instant → one deduplicated timer; the
        // expiry-index list is what lets that single timer clear both keys.
        assertEquals(1, harness.numEventTimeTimers(), "same expiry instant dedupes to one timer");

        harness.processWatermark(T0 + TTL_MS);
        assertEquals(0, harness.numKeyedStateEntries(), "shared expiry timer must clear every listed key");
        assertEquals(0, harness.numEventTimeTimers());
    }
}
