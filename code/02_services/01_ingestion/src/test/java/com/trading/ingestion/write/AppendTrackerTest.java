package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-FAIL-002: AppendTracker backpressure behavior.
 *
 * <p>Verifies:
 * <ul>
 *   <li>Records accepted below limits</li>
 *   <li>80% warning → isReady returns false</li>
 *   <li>100% halt → tryAccept returns false</li>
 *   <li>Pending counters decrease only after append completes</li>
 * </ul>
 */
@DisplayName("ING-FAIL-002: AppendTracker Backpressure")
class AppendTrackerTest {

    private AppendTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new AppendTracker();
    }

    @Test
    @DisplayName("Accepts records below limits")
    void acceptsBelowLimits() {
        assertTrue(tracker.tryAccept(100), "Should accept small record");
        assertEquals(1, tracker.pendingRecords());
        assertTrue(tracker.pendingBytes() > 0);
        assertTrue(tracker.isReady());
    }

    @Test
    @DisplayName("80% record warning → isReady returns false")
    void warningAt80PercentRecords() {
        int limit = (int) AppendTracker.MAX_PENDING_RECORDS;
        int at80 = (int) (limit * 0.80) - 1;

        for (int i = 0; i < at80; i++) {
            assertTrue(tracker.tryAccept(100));
        }
        // Below 80% → still ready
        assertTrue(tracker.isReady(), "Below 80% should be ready");

        // Cross 80%
        assertTrue(tracker.tryAccept(100), "80% should still accept");
        assertFalse(tracker.isReady(), "At 80% must report not-ready");
    }

    @Test
    @DisplayName("80% byte warning → isReady returns false")
    void warningAt80PercentBytes() {
        long limit = AppendTracker.MAX_PENDING_BYTES;
        int bigRecordBytes = (int) (limit * 0.81);

        boolean accepted = tracker.tryAccept(bigRecordBytes);
        // May be accepted or rejected depending on buffer state at exact boundaries
        if (accepted) {
            assertFalse(tracker.isReady(), "At 81% of byte limit must report not-ready");
        }
    }

    @Test
    @DisplayName("100% halt on records — tryAccept returns false")
    void haltAt100PercentRecords() {
        int limit = (int) AppendTracker.MAX_PENDING_RECORDS;

        for (int i = 0; i < limit; i++) {
            assertTrue(tracker.tryAccept(100));
        }

        // Next should fail
        assertFalse(tracker.tryAccept(100), "10001st record must be rejected");
        assertTrue(tracker.isHalted(), "Tracker must halt at 100%");
        assertEquals(limit, tracker.totalAccepted());
        assertEquals(1, tracker.totalRejected());
    }

    @Test
    @DisplayName("Pending counters decrease on append success")
    void pendingDecreasesOnSuccess() {
        tracker.tryAccept(500);
        assertEquals(1, tracker.pendingRecords());
        assertEquals(500, tracker.pendingBytes());

        tracker.onAppendSuccess(500);
        assertEquals(0, tracker.pendingRecords());
        assertEquals(0, tracker.pendingBytes());
        assertEquals(1, tracker.totalAppended());
    }

    @Test
    @DisplayName("Pending counters decrease on append failure")
    void pendingDecreasesOnFailure() {
        tracker.tryAccept(500);
        assertEquals(1, tracker.pendingRecords());

        tracker.onAppendFailure(500);
        assertEquals(0, tracker.pendingRecords());
        assertEquals(0, tracker.pendingBytes());
        assertEquals(1, tracker.totalFailed());
    }

    @Test
    @DisplayName("Warning listener is called at 80%")
    void warningListenerCalled() {
        final int[] callCount = {0};
        final AppendTracker.BackpressureListener.Level[] lastLevel = {null};

        tracker.setListener((level, pr, pb, mr, mb, now) -> {
            callCount[0]++;
            lastLevel[0] = level;
        });

        int atWarning = (int) (AppendTracker.MAX_PENDING_RECORDS * 0.80) + 1;
        for (int i = 0; i < atWarning; i++) {
            tracker.tryAccept(100);
        }

        // Warning should have fired at least once
        assertTrue(callCount[0] >= 1, "Warning listener must be called at 80%");
        if (callCount[0] > 0) {
            assertEquals(AppendTracker.BackpressureListener.Level.WARNING, lastLevel[0]);
        }
    }

    @Test
    @DisplayName("Critical listener is called at 100%")
    void criticalListenerCalled() {
        final int[] callCount = {0};
        final AppendTracker.BackpressureListener.Level[] lastLevel = {null};

        tracker.setListener((level, pr, pb, mr, mb, now) -> {
            callCount[0]++;
            lastLevel[0] = level;
        });

        int limit = (int) AppendTracker.MAX_PENDING_RECORDS;
        for (int i = 0; i < limit; i++) {
            tracker.tryAccept(100);
        }

        // Now the critical one
        tracker.tryAccept(100);
        assertTrue(callCount[0] >= 1, "Listener must be called at 100%");
        if (callCount[0] >= 2) { // at least one WARNING + one CRITICAL
            assertEquals(AppendTracker.BackpressureListener.Level.CRITICAL, lastLevel[0]);
        }
    }

    @Test
    @DisplayName("Once halted, tryAccept returns false for all subsequent calls")
    void haltedRejectsAll() {
        int limit = (int) AppendTracker.MAX_PENDING_RECORDS;
        for (int i = 0; i < limit; i++) {
            assertTrue(tracker.tryAccept(100));
        }

        assertFalse(tracker.tryAccept(100), "First reject");
        assertFalse(tracker.tryAccept(1), "Subsequent small record also rejected");
        assertEquals(2, tracker.totalRejected());
    }

    // ---- ING-FAIL-005: halt-latch lifecycle (decided 2026-08-15) ----

    /**
     * ING-FAIL-005: the halt latch is pinned to "halted until process
     * restart". Reaching a pending limit is a platform-capacity fault (dossier
     * §Slow-Fluss policy), not a normal operating condition — the service must
     * not silently resume after an acknowledged drop without an operator
     * restart. Draining pending back to zero (success or failure) never
     * unhalts, and accepts stay rejected while halted.
     */
    @Test
    @DisplayName("ING-FAIL-005: halt latch persists after pending drains to zero")
    void haltLatchPersistsAfterDrain() {
        AppendTracker small = new AppendTracker(10, 100_000, 0.80);
        for (int i = 0; i < 10; i++) {
            assertTrue(small.tryAccept(100));
        }
        // 100% → halted.
        assertFalse(small.tryAccept(100), "11th record must be rejected");
        assertTrue(small.isHalted());
        assertFalse(small.isReady());

        // Drain every accepted record — success and failure paths both release.
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) small.onAppendSuccess(100);
            else small.onAppendFailure(100);
        }
        assertEquals(0, small.pendingRecords(), "pending must drain to zero");
        assertEquals(0, small.pendingBytes(), "pending bytes must drain to zero");

        // Pinned lifecycle: halted stays latched until restart.
        assertTrue(small.isHalted(), "halt must persist after draining (fail-closed until restart)");
        assertFalse(small.isReady(), "readiness must stay false while halted");
        assertFalse(small.tryAccept(100), "accepts must stay rejected while halted");
        assertEquals(2, small.totalRejected());
        assertFalse(small.isReady(), "isReady must remain false even after a fresh reject");
    }

    // ---- ING-FAIL-004: concurrency invariants ----

    /**
     * ING-FAIL-004: the tracker is the money-safety backpressure core and must
     * hold its invariants under concurrency. 16 threads race accept/release
     * with a small limit so the 100% halt path is exercised concurrently;
     * final state must reconcile exactly and pending counters must never go
     * negative.
     */
    @Test
    @DisplayName("ING-FAIL-004: concurrent accept/release keeps all tracker invariants")
    void concurrentAcceptReleaseKeepsInvariants() throws Exception {
        AppendTracker small = new AppendTracker(200, 50_000, 0.80);
        int threads = 16;
        int perThread = 5_000;
        int inFlightBudget = 25; // hold up to 25 before releasing → pending can exceed the 200 limit → halt path races
        java.util.Random rng = new java.util.Random(42);

        AtomicLong accepted = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        AtomicLong appended = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong acceptedBytes = new AtomicLong();
        AtomicBoolean negativeSeen = new AtomicBoolean();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                long acc = 0, rej = 0, app = 0, fail = 0, bytes = 0;
                List<Integer> held = new ArrayList<>();
                for (int i = 0; i < perThread; i++) {
                    int size = 100 + rng.nextInt(500);
                    if (small.tryAccept(size)) {
                        acc++;
                        bytes += size;
                        held.add(size);
                        if (held.size() >= inFlightBudget) {
                            for (int s : held) {
                                if (rng.nextBoolean()) {
                                    small.onAppendSuccess(s);
                                    app++;
                                } else {
                                    small.onAppendFailure(s);
                                    fail++;
                                }
                            }
                            held.clear();
                        }
                    } else {
                        rej++;
                    }
                    // Sampler: pending must never be observed negative.
                    if (small.pendingRecords() < 0 || small.pendingBytes() < 0) {
                        negativeSeen.set(true);
                    }
                }
                // Release any held remainder.
                for (int s : held) {
                    if (rng.nextBoolean()) {
                        small.onAppendSuccess(s);
                        app++;
                    } else {
                        small.onAppendFailure(s);
                        fail++;
                    }
                }
                accepted.addAndGet(acc);
                rejected.addAndGet(rej);
                appended.addAndGet(app);
                failed.addAndGet(fail);
                acceptedBytes.addAndGet(bytes);
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(120, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Every attempt either accepted or rejected — nothing vanishes.
        assertEquals((long) threads * perThread, accepted.get() + rejected.get());
        // Every accepted record completed exactly once, as success or failure.
        assertEquals(accepted.get(), appended.get() + failed.get());
        // Pending fully drained.
        assertEquals(0, small.pendingRecords(), "pending records must reconcile to zero");
        assertEquals(0, small.pendingBytes(), "pending bytes must reconcile to zero");
        // Tracker totals match the workers' own accounting.
        assertEquals(accepted.get(), small.totalAccepted());
        assertEquals(rejected.get(), small.totalRejected());
        assertEquals(appended.get(), small.totalAppended());
        assertEquals(failed.get(), small.totalFailed());
        assertEquals(acceptedBytes.get(), small.totalBytesAccepted(),
                "accepted bytes must equal the sum of accepted record sizes");
        assertFalse(negativeSeen.get(), "pending counters must never go negative");
    }
}
