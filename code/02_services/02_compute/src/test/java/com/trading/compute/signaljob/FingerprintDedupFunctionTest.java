package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

    // ── tracker 14 P5.3 hot-path boundary tests ───────────────────────────

    @Test
    void differentFingerprintVersionDoesNotCollide() throws Exception {
        openHarness();
        GenericRowData v1 = TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1);
        v1.setField(RawTableColumns.FINGERPRINT_VERSION, StringData.fromString("1"));
        process(v1);
        GenericRowData v2 = TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 1);
        v2.setField(RawTableColumns.FINGERPRINT_VERSION, StringData.fromString("2"));
        process(v2); // same fingerprint, different version → distinct state key

        assertEquals(2, emittedCount(harness), "version is part of the state key — v2 must pass");
    }

    @Test
    void stateDoesNotGrowWhenAllRowsAreDuplicates() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        for (int i = 0; i < 10_000; i++) {
            process(TestRawRows.row(1L, T0 + i, "fp-1", "TRADE", 100 + i, i));
        }
        assertEquals(1, ComputeOtlpEmitter.dedupStateCount(),
                "10k duplicates of one fingerprint must not grow state");
        assertEquals(1, harness.numEventTimeTimers());
    }

    @Test
    void idleSourceDoesNotAccumulateState() throws Exception {
        openHarness();
        // Nothing processed: the operator must add no state and no timers of
        // its own — an idle source cannot grow the dedup map.
        assertEquals(0, harness.numKeyedStateEntries());
        assertEquals(0, harness.numEventTimeTimers());
    }

    @Test
    void highCardinalityFingerprintsAllPass() throws Exception {
        openHarness();
        int n = 5_000;
        for (int i = 0; i < n; i++) {
            process(TestRawRows.row(1L, T0 + i, "fp-hc-" + i, "TRADE", 100 + i, i));
        }
        assertEquals(n, emittedCount(harness), "every distinct fingerprint passes");
        assertEquals(n, ComputeOtlpEmitter.dedupStateCount(),
                "state holds exactly one entry per live fingerprint");
    }

    @Test
    void malformedEmptyFingerprintIsScopedNotCrashed() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "", "TRADE", 101, 2));

        assertEquals(1, emittedCount(harness),
                "empty fingerprint is a valid (if malformed) state key — first passes, duplicate drops");
        assertEquals(1, ComputeOtlpEmitter.dedupStateCount());
    }

    @Test
    void eventTimeOverflowExpiryClampsToMaxValue() throws Exception {
        openHarness();
        long nearMax = Long.MAX_VALUE - 1_000L;
        GenericRowData row = TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1);
        row.setField(RawTableColumns.EVENT_TIME, nearMax);
        process(row); // nominal expiry would wrap negative — must clamp, not crash

        assertEquals(1, emittedCount(harness));
        assertEquals(1, ComputeOtlpEmitter.dedupStateCount());
        assertEquals(1, harness.numEventTimeTimers(),
                "clamped MAX_VALUE timer registered once");
        // Advance the watermark beyond the clamped instant — the timer fires,
        // the entry is cleared (a negative expiry would have fired immediately
        // at open and dropped the entry before the watermark ever moved).
        harness.processWatermark(Long.MAX_VALUE);
        assertEquals(0, ComputeOtlpEmitter.dedupStateCount());
    }

    // ── tracker 14 P5.1 gauge mirrors ─────────────────────────────────────

    @BeforeEach
    void resetGaugeMirrors() {
        // JVM-wide statics — every test starts from a clean mirror.
        ComputeOtlpEmitter.resetDedupGaugesForTest();
        ComputeOtlpEmitter.resetDedupTelemetryForTest();
    }

    @Test
    void dedupGaugesTrackInsertAndExpiry() throws Exception {
        openHarness(); // gauges register at open; mirrors start at zero
        assertEquals(0, ComputeOtlpEmitter.dedupStateCount());
        assertEquals(0, ComputeOtlpEmitter.dedupExpiryIndexCount());
        assertEquals(0, ComputeOtlpEmitter.dedupBytesEstimate());

        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-2", "TRADE", 101, 2));
        process(TestRawRows.row(1L, T0 + 2_000L, "fp-1", "TRADE", 102, 3)); // duplicate

        assertEquals(2, ComputeOtlpEmitter.dedupStateCount(),
                "two live fingerprints, duplicate adds nothing");
        assertEquals(2, ComputeOtlpEmitter.dedupExpiryIndexCount());
        assertEquals(2 * FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES
                        + 2 * FingerprintDedupFunction.PER_BUCKET_ESTIMATE_BYTES,
                ComputeOtlpEmitter.dedupBytesEstimate());

        harness.processWatermark(T0 + TTL_MS);
        assertEquals(1, ComputeOtlpEmitter.dedupStateCount(),
                "first expiry removes fp-1; fp-2's timer is later");
        assertEquals(1, ComputeOtlpEmitter.dedupExpiryIndexCount());

        harness.processWatermark(T0 + 1_000L + TTL_MS);
        assertEquals(0, ComputeOtlpEmitter.dedupStateCount(), "expiry clears the mirrors too");
        assertEquals(0, ComputeOtlpEmitter.dedupExpiryIndexCount());
        assertEquals(0, ComputeOtlpEmitter.dedupBytesEstimate());
    }

    @Test
    void dedupGaugesFoldRestoredStateExactly() throws Exception {
        openHarness();
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        assertEquals(1, ComputeOtlpEmitter.dedupStateCount());

        // Checkpoint the harness, then restore it into a NEW operator — the
        // restored MapState holds fp-1 but the fresh mirrors start at zero.
        OperatorSubtaskState state = harness.snapshot(0L, 0L);
        harness.close();
        ComputeOtlpEmitter.resetDedupGaugesForTest();

        // forKeyedProcessFunction already initializes the harness (empty) —
        // a RESTORE needs a manually constructed harness: initializeState
        // must run before open.
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> restored =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(
                                new FingerprintDedupFunction(SignalJobConfig.from(env()))),
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG);
        restored.initializeState(state);
        restored.open();
        try {
            // First row for token 1: replace-tracked(0) with actual-restored(1).
            processOn(restored, TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));
            assertEquals(1, ComputeOtlpEmitter.dedupStateCount(),
                    "restored fp-1 folded in exactly (replace, not add)");

            // A brand-new fingerprint adds one.
            processOn(restored, TestRawRows.row(1L, T0 + 2_000L, "fp-2", "TRADE", 102, 3));
            assertEquals(2, ComputeOtlpEmitter.dedupStateCount());

            // Restored fp-1 is still deduplicated.
            processOn(restored, TestRawRows.row(1L, T0 + 3_000L, "fp-1", "TRADE", 103, 4));
            assertEquals(2, ComputeOtlpEmitter.dedupStateCount());
        } finally {
            restored.close();
        }
    }

    private static void processOn(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h, GenericRowData row)
            throws Exception {
        h.processElement(row, row.getLong(RawTableColumns.EVENT_TIME));
    }

    // ── DEC-038 bounded-checkpoint invariant: eviction deletes the timer ──

    @Test
    void evictionKeepsTimerStateBoundedByCacheCap() throws Exception {
        // Small entry cap so eviction is the steady state; bytes cap stays
        // high so the entry bound governs (min semantics).
        Map<String, String> env = env();
        env.put("DEDUP_CACHE_MAX_ENTRIES", "8");
        InMemoryFingerprintDedupStateStore store = new InMemoryFingerprintDedupStateStore();
        SignalJobConfig config = SignalJobConfig.from(env);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new FingerprintDedupFunction(config, () -> store),
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();

        // 100 distinct fingerprints with distinct event times → 100 distinct
        // expiry instants → 100 event-time timers WOULD accumulate without the
        // eviction timer-deletion fix (SIG-STATE-001, DEC-038 hard invariant:
        // checkpoint must not grow with Fluss dedup cardinality). With the fix,
        // evicting a bucket deletes its timer, so the live timer count stays ≤
        // the cache cap.
        for (int i = 0; i < 100; i++) {
            process(TestRawRows.row(1L, T0 + i * 1_000L, "fp-evict-" + i, "TRADE", 100 + i, i));
        }

        assertTrue(harness.numEventTimeTimers() <= 8,
                "evicted buckets' timers must be deleted (bounded checkpoint): timers="
                        + harness.numEventTimeTimers() + " cap=8");
        assertTrue(ComputeOtlpEmitter.dedupStateCount() <= 8,
                "cache entry count bounded by the cap");
        assertTrue(ComputeOtlpEmitter.dedupExpiryIndexCount() <= 8,
                "expiry-index bucket count bounded by the cap");

        // Model the real writer→sink path: the dedup function only EMITS
        // first-seen rows on its side output; the FingerprintDedupWriterFunction
        // + fingerprint_dedup sink perform the durable putFirstSeen. Push the
        // side-output rows into the store so the authoritative set holds all
        // 100 accepted fingerprints.
        ConcurrentLinkedQueue<StreamRecord<RowData>> writeOut = harness.getSideOutput(
                FingerprintDedupFunction.DEDUP_WRITE_OUTPUT);
        for (StreamRecord<RowData> r : writeOut) {
            RowData row = r.getValue();
            store.putFirstSeen(
                    row.getLong(FingerprintDedupTableColumns.INSTRUMENT_TOKEN),
                    row.getString(FingerprintDedupTableColumns.FINGERPRINT_VERSION).toString(),
                    row.getString(FingerprintDedupTableColumns.EVENT_FINGERPRINT).toString(),
                    row.getLong(FingerprintDedupTableColumns.FIRST_SEEN_MS),
                    row.getLong(FingerprintDedupTableColumns.EXPIRY_MS));
        }
        assertEquals(100, store.rowCount(),
                "all 100 first-seen rows are durable in the store (the writer path); "
                        + "the bounded cache is only the working state");

        // An EVICTED fingerprint re-arrives inside its TTL: cache miss →
        // authoritative store SEEN_LIVE → still deduped (correctness intact,
        // eviction never widens the dedup window).
        long emittedBefore = emittedCount(harness);
        process(TestRawRows.row(1L, T0, "fp-evict-0", "TRADE", 200, 1000));
        assertEquals(emittedBefore, emittedCount(harness),
                "re-delivery of an evicted in-TTL fingerprint must still be dropped");
    }

    // ── DEC-038 telemetry: cache hit/miss + rehydration latency/failures ──

    @Test
    void dedupCacheTelemetryTracksHotPathDecisions() throws Exception {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        openHarness();
        // First occurrence: cache miss (store consulted) → accept. Re-delivery:
        // cache hit (definite duplicate) → drop. No further store round trip.
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));

        assertEquals(1, emitter.drainDedupCacheMissesDelta(),
                "first occurrence is a cache miss (query-on-miss rehydration)");
        assertEquals(1, emitter.drainDedupCacheHitsDelta(),
                "in-TTL re-delivery is a cache-only definite duplicate");
        assertTrue(ComputeOtlpEmitter.rehydrationLatencyMsForTest() >= 0L,
                "the store lookup is timed (rehydration latency gauge)");
        assertEquals(0, emitter.drainDedupRehydrationFailuresDelta(),
                "no store failures on the healthy path");
    }

    @Test
    void rehydrationFailureFailsClosedAndIsCounted() throws Exception {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        // Store whose lookup throws — the authoritative read must fail the
        // task (SIG-STATE-003, never an empty dedup set) AFTER counting the
        // failure so it is observable.
        FingerprintDedupStateStore failingStore = new FingerprintDedupStateStore() {
            @Override
            public Lookup lookup(long token, String version, String fingerprint, long nowMs) {
                throw new IllegalStateException("fingerprint_dedup unavailable");
            }

            @Override
            public void putFirstSeen(long token, String version, String fingerprint,
                    long firstSeenMs, long expiryMs) {}

            @Override
            public List<DedupExpiry.CleanupCandidate> scanExpired(
                    long token, long nowMs, int maxBatchSize) {
                return List.of();
            }

            @Override
            public void delete(List<DedupExpiry.CleanupCandidate> batch) {}

            @Override
            public void close() {}
        };
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new FingerprintDedupFunction(config, () -> failingStore),
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();

        assertThrows(IllegalStateException.class,
                () -> process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1)),
                "a store failure must fail the task, never silently accept");
        assertEquals(1, emitter.drainDedupRehydrationFailuresDelta(),
                "the failure is counted before the task fails closed");
        assertTrue(ComputeOtlpEmitter.rehydrationLatencyMsForTest() >= 0L,
                "the failed lookup is still timed");
    }
}
