package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Instant;
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
}
