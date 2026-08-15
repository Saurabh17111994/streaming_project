package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
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
 * DEC-038 externalized dedup wiring (docs/08_implementation/04-signal-job.md
 * §Design — fingerprint_dedup dedup state table): the authoritative decision is
 * the Fluss-backed store, the bounded Flink cache is never the sole source of
 * truth, first-seen rows leave via the {@code fingerprint-dedup-write} side
 * output, and the processing-time cleanup pass deletes only expired store rows.
 *
 * <p>Harness-driven (no cluster) with the shared in-memory store — the same
 * {@link FingerprintDedupStateStore} contract the raw-client twin implements.
 */
class FingerprintDedupExternalizationTest {

    private static final long TTL_MS = 300_000L;
    private static final long T0 = 1_700_000_000_000L;
    private static final long TOKEN = 7L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
        ComputeOtlpEmitter.resetDedupGaugesForTest();
    }

    private InMemoryFingerprintDedupStateStore openWithStore(Map<String, String> extra)
            throws Exception {
        InMemoryFingerprintDedupStateStore store = new InMemoryFingerprintDedupStateStore();
        SignalJobConfig config = SignalJobConfig.from(env(extra));
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new FingerprintDedupFunction(config, () -> store),
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
        // The harness processing clock starts at 0; anchor it at a realistic
        // wall time so T0-based store fixtures are live/expired as intended.
        harness.setProcessingTime(T0);
        return store;
    }

    private void process(GenericRowData row) throws Exception {
        harness.processElement(row, row.getLong(RawTableColumns.EVENT_TIME));
    }

    private ConcurrentLinkedQueue<StreamRecord<RowData>> writeOutput() {
        ConcurrentLinkedQueue<StreamRecord<RowData>> q =
                harness.getSideOutput(FingerprintDedupFunction.DEDUP_WRITE_OUTPUT);
        return q == null ? new ConcurrentLinkedQueue<>() : q;
    }

    private long mainEmitted() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord).count();
    }

    private static Map<String, String> env(Map<String, String> extra) {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        if (extra != null) {
            env.putAll(extra);
        }
        return env;
    }

    @Test
    void firstSeenEmitsDurableWriteDuplicateDoesNot() throws Exception {
        openWithStore(null);
        process(TestRawRows.row(TOKEN, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(TOKEN, T0 + 1_000L, "fp-1", "TRADE", 101, 2));

        assertEquals(1, mainEmitted(), "duplicate within TTL dropped from the main path");
        assertEquals(1, writeOutput().size(),
                "exactly one durable write for the first occurrence");

        StreamRecord<RowData> write = writeOutput().peek();
        RowData row = write.getValue();
        assertEquals(TOKEN, row.getLong(FingerprintDedupTableColumns.INSTRUMENT_TOKEN));
        assertEquals("v2", row.getString(FingerprintDedupTableColumns.FINGERPRINT_VERSION)
                .toString());
        assertEquals("fp-1", row.getString(FingerprintDedupTableColumns.EVENT_FINGERPRINT)
                .toString());
        assertEquals(T0, row.getLong(FingerprintDedupTableColumns.FIRST_SEEN_MS));
        assertEquals(DedupExpiry.expiryMs(T0, TTL_MS),
                row.getLong(FingerprintDedupTableColumns.EXPIRY_MS),
                "expiry must be exactly first_seen + TTL");
        assertEquals(FingerprintDedupTableColumns.SCHEMA_VERSION_V1,
                row.getString(FingerprintDedupTableColumns.SCHEMA_VERSION).toString());
    }

    @Test
    void storeLiveDuplicateDroppedAndCacheWarmed() throws Exception {
        InMemoryFingerprintDedupStateStore store = openWithStore(null);
        // Authoritative store says this fingerprint was already seen (live).
        store.putFirstSeen(TOKEN, "v2", "fp-live", T0 - 60_000L, T0 + TTL_MS);

        process(TestRawRows.row(TOKEN, T0, "fp-live", "TRADE", 100, 1));

        assertEquals(0, mainEmitted(), "live store row is a duplicate — dropped");
        assertEquals(0, writeOutput().size(), "no durable write for a seen fingerprint");
        // The decision warmed the cache — a repeat must not re-query the store.
        process(TestRawRows.row(TOKEN, T0 + 1_000L, "fp-live", "TRADE", 101, 2));
        assertEquals(0, mainEmitted());
    }

    @Test
    void storeExpiredRowReAcceptedAndRefreshed() throws Exception {
        InMemoryFingerprintDedupStateStore store = openWithStore(null);
        // Stale row (logically expired): never a false "seen" after expiry.
        store.putFirstSeen(TOKEN, "v2", "fp-stale", T0 - 1_000_000L, T0 - 1_000L);

        process(TestRawRows.row(TOKEN, T0, "fp-stale", "TRADE", 100, 1));

        assertEquals(1, mainEmitted(), "expired fingerprint is eligible again");
        assertEquals(1, writeOutput().size(), "re-accept refreshes the durable row");
    }

    @Test
    void cleanupTimerDeletesOnlyExpiredStoreRows() throws Exception {
        InMemoryFingerprintDedupStateStore store = openWithStore(null);
        store.putFirstSeen(TOKEN, "v2", "fp-expired", T0 - 60_000L, T0 - 1_000L);
        store.putFirstSeen(TOKEN, "v2", "fp-live", T0 - 60_000L, T0 + TTL_MS);

        process(TestRawRows.row(TOKEN, T0, "fp-trigger", "TRADE", 100, 1));
        // Grid-aligned cleanup timer fires at the next 60 s boundary (default
        // DEDUP_CLEANUP_INTERVAL_MS) after the current processing time.
        harness.setProcessingTime(harness.getProcessingTime() + 60_000L);

        assertTrue(store.deletedKeys().stream().anyMatch(k -> k.contains("fp-expired")),
                "expired row must be deleted by the cleanup pass");
        assertTrue(store.deletedKeys().stream().noneMatch(k -> k.contains("fp-live")),
                "live row must never be deleted");
        assertEquals(1, store.rowCount(), "exactly the live row retained");
    }

    @Test
    void cacheEvictionKeepsStoreAuthoritative() throws Exception {
        InMemoryFingerprintDedupStateStore store = openWithStore(
                Map.of("DEDUP_CACHE_MAX_ENTRIES", "3"));
        for (int i = 1; i <= 5; i++) {
            process(TestRawRows.row(TOKEN, T0 + i * 1_000L, "fp-e-" + i, "TRADE", 100 + i, i));
        }

        // Cache bounded at 3; the durable rows land in the store via the write
        // path (the writer operator + sink — simulated here with the direct
        // store puts, same contract).
        assertEquals(5, mainEmitted());
        assertEquals(5, writeOutput().size(), "every first-seen is durable");
        assertEquals(3, ComputeOtlpEmitter.dedupStateCount(),
                "cache bounded to DEDUP_CACHE_MAX_ENTRIES");
        for (int i = 1; i <= 5; i++) {
            store.putFirstSeen(TOKEN, "v2", "fp-e-" + i,
                    T0 + i * 1_000L, T0 + i * 1_000L + TTL_MS);
        }

        // Re-process an evicted fingerprint: cache miss -> authoritative store
        // says SEEN_LIVE -> dropped, with no new durable write.
        process(TestRawRows.row(TOKEN, T0 + 1_000L, "fp-e-1", "TRADE", 500, 99));
        assertEquals(5, mainEmitted(), "evicted fingerprint still deduplicated via the store");
        assertEquals(5, writeOutput().size(), "no refresh write for a seen fingerprint");
    }

    @Test
    void distinctVersionsIndependentInStore() throws Exception {
        InMemoryFingerprintDedupStateStore store = openWithStore(null);
        store.putFirstSeen(TOKEN, "v2", "fp-x", T0, T0 + TTL_MS);

        GenericRowData v2 = TestRawRows.row(TOKEN, T0, "fp-x", "TRADE", 100, 1);
        v2.setField(RawTableColumns.FINGERPRINT_VERSION, StringData.fromString("2"));
        process(v2);

        assertEquals(1, mainEmitted(), "same fingerprint, different version — distinct key");
        assertEquals(1, writeOutput().size());
    }
}
