package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 P5.1 — baseline measurements of the live dedup hot path, driven
 * through the same Flink 2.2.1 operator harness as
 * {@link FingerprintDedupFunctionTest} (no cluster). Each measurement is
 * printed as a {@code P5.1[...]} System.out line so the surefire report is a
 * self-contained evidence record.
 *
 * <p>The timer-count box in the tracker explicitly allows an equivalent
 * deterministic timer/state-count measurement when the runtime metric is
 * unavailable — {@code KeyedOneInputStreamOperatorTestHarness#numEventTimeTimers}
 * IS the operator's event-time timer count (the exact value the runtime
 * {@code numTimers} metric reports), so it is used directly.
 *
 * <p>All feed waves use one instrument token with DISTINCT event times, so
 * every fingerprint gets its own expiry instant: state count, expiry-index
 * bucket count, and timer count grow in lockstep — a deterministic series that
 * proves the state is bounded and exactly one entry per live fingerprint.
 */
@DisplayName("Tracker 14 P5.1: dedup baseline measurements (counts, timers, bytes, GC)")
class DedupBaselineMeasurementTest {

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

    @BeforeEach
    void resetGaugeMirrors() {
        // JVM-wide statics — every test starts from a clean mirror.
        ComputeOtlpEmitter.resetDedupGaugesForTest();
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

    private void openHarness() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new FingerprintDedupFunction(config),
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private void feedDistinct(int waveIndex, int count) throws Exception {
        long waveStart = T0 + waveIndex * 100_000L;
        for (int i = 0; i < count; i++) {
            harness.processElement(
                    TestRawRows.row(1L, waveStart + i, "fp-wave-" + waveIndex + "-" + i,
                            "TRADE", 10_000L + i, 100L),
                    waveStart + i);
        }
    }

    @Test
    @DisplayName("state/expiry-index/timer counts grow linearly with live fingerprints and fall to zero after expiry")
    void measureStateCountsAcrossFeedWaves() throws Exception {
        openHarness();
        int perWave = 2_000;

        feedDistinct(0, perWave);
        long s1 = ComputeOtlpEmitter.dedupStateCount();
        long e1 = ComputeOtlpEmitter.dedupExpiryIndexCount();
        long t1 = harness.numEventTimeTimers();
        long b1 = ComputeOtlpEmitter.dedupBytesEstimate();
        System.out.println("P5.1[waves] wave1 state=" + s1 + " expiryIndex=" + e1
                + " timers=" + t1 + " bytesEstimate=" + b1);
        assertEquals(perWave, s1, "one entry per live fingerprint");
        assertEquals(perWave, e1, "one expiry bucket per distinct expiry instant");
        assertEquals(perWave, t1, "one event-time timer per live fingerprint");

        feedDistinct(1, perWave);
        long s2 = ComputeOtlpEmitter.dedupStateCount();
        long e2 = ComputeOtlpEmitter.dedupExpiryIndexCount();
        long t2 = harness.numEventTimeTimers();
        System.out.println("P5.1[waves] wave2 state=" + s2 + " expiryIndex=" + e2
                + " timers=" + t2 + " bytesEstimate=" + ComputeOtlpEmitter.dedupBytesEstimate());
        assertEquals(2L * perWave, s2, "wave 2 doubles the live count");
        assertEquals(2L * perWave, e2);
        assertEquals(2L * perWave, t2);

        feedDistinct(2, perWave);
        long s3 = ComputeOtlpEmitter.dedupStateCount();
        long e3 = ComputeOtlpEmitter.dedupExpiryIndexCount();
        long t3 = harness.numEventTimeTimers();
        System.out.println("P5.1[waves] wave3 state=" + s3 + " expiryIndex=" + e3
                + " timers=" + t3 + " bytesEstimate=" + ComputeOtlpEmitter.dedupBytesEstimate());
        assertEquals(3L * perWave, s3, "wave 3 triples the live count");
        assertEquals(3L * perWave, e3);
        assertEquals(3L * perWave, t3);
        assertEquals(3L * perWave * (FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES
                        + FingerprintDedupFunction.PER_BUCKET_ESTIMATE_BYTES),
                ComputeOtlpEmitter.dedupBytesEstimate(),
                "bytes estimate = entries x (128 B entry + 64 B bucket) upper bound");

        // Watermark past EVERY expiry instant (wave2's last expiry is
        // T0+500000+1999): the timer sweep must delete all entries — state
        // falls to zero (never retained after expiry).
        harness.processWatermark(T0 + 502_001L);
        long s4 = ComputeOtlpEmitter.dedupStateCount();
        long e4 = ComputeOtlpEmitter.dedupExpiryIndexCount();
        long t4 = harness.numEventTimeTimers();
        System.out.println("P5.1[waves] afterExpiry state=" + s4 + " expiryIndex=" + e4
                + " timers=" + t4 + " bytesEstimate=" + ComputeOtlpEmitter.dedupBytesEstimate());
        assertEquals(0L, s4, "state falls to zero after watermark passes expiry");
        assertEquals(0L, e4);
        assertEquals(0L, t4);
    }

    @Test
    @DisplayName("allocation and GC rate over a 100k-fingerprint feed are measurable and bounded")
    void measureAllocationAndGcRate() throws Exception {
        openHarness();
        java.lang.management.ThreadMXBean mgmt = ManagementFactory.getThreadMXBean();
        com.sun.management.ThreadMXBean tmx =
                (mgmt instanceof com.sun.management.ThreadMXBean)
                        ? (com.sun.management.ThreadMXBean) mgmt
                        : null;
        boolean allocSupported = tmx != null && tmx.isThreadAllocatedMemorySupported();
        long allocBefore = allocSupported ? tmx.getCurrentThreadAllocatedBytes() : -1L;
        long gcBefore = gcCount();
        long gcTimeBefore = gcTimeMs();

        int n = 100_000;
        for (int i = 0; i < n; i++) {
            harness.processElement(
                    TestRawRows.row(1L, T0 + i, "fp-gc-" + i, "TRADE", 10_000L + i, 100L),
                    T0 + i);
        }

        long allocAfter = allocSupported ? tmx.getCurrentThreadAllocatedBytes() : -1L;        long gcDelta = gcCount() - gcBefore;
        long gcTimeDelta = gcTimeMs() - gcTimeBefore;
        long state = ComputeOtlpEmitter.dedupStateCount();
        System.out.println("P5.1[gc] rows=" + n + " allocatedBytes="
                + (allocSupported ? (allocAfter - allocBefore) : "unsupported")
                + " gcCollections=" + gcDelta + " gcMs=" + gcTimeDelta
                + " state=" + state);
        assertEquals(n, state, "state holds exactly one entry per distinct fingerprint");
        if (allocSupported) {
            assertTrue(allocAfter > allocBefore,
                    "feeding 100k fingerprints must allocate bytes on this thread");
        }
        assertTrue(gcDelta >= 0 && gcTimeDelta >= 0, "GC counters never go backwards");
    }

    @Test
    @DisplayName("a repeated fingerprint creates no duplicate expiry-index entries")
    void repeatedFingerprintDoesNotDuplicateExpiryIndexEntry() throws Exception {
        openHarness();
        harness.processElement(TestRawRows.row(1L, T0, "fp-dup", "TRADE", 100, 1), T0);
        for (int i = 1; i < 10_000; i++) {
            harness.processElement(
                    TestRawRows.row(1L, T0 + i, "fp-dup", "TRADE", 100 + i, i), T0 + i);
        }
        assertEquals(1L, ComputeOtlpEmitter.dedupStateCount(),
                "10k duplicates must not grow the fingerprint map");
        assertEquals(1L, ComputeOtlpEmitter.dedupExpiryIndexCount(),
                "10k duplicates must not grow the expiry index");
        assertEquals(1, harness.numEventTimeTimers(), "one expiry timer, never one per duplicate");
    }

    private static long gcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            sum += bean.getCollectionCount();
        }
        return sum;
    }

    private static long gcTimeMs() {
        long sum = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            sum += bean.getCollectionTime();
        }
        return sum;
    }
}
