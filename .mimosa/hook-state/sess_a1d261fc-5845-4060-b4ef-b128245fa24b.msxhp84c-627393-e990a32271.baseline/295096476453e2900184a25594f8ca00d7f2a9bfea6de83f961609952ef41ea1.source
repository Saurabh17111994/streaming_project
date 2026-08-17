package com.trading.ingestion.discontinuity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-FAIL-007: TIME_JUMP discontinuity emission decision.
 *
 * <p>The monitor emits at most one TIME_JUMP per clock-violation episode:
 * the first measurement whose absolute offset exceeds the limit. A stuck
 * clock must not spam discontinuity rows; recovery (within limit) resets the
 * episode so a later violation emits again.
 */
@DisplayName("ING-FAIL-007: TimeJumpMonitor — one TIME_JUMP per violation episode")
class TimeJumpMonitorTest {

    private static final long LIMIT = 100L;

    @Test
    @DisplayName("first over-limit measurement emits")
    void firstViolationEmits() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        assertTrue(m.onOffsetMeasured(150), "first over-limit measurement must emit");
        assertTrue(m.isViolated());
    }

    @Test
    @DisplayName("repeated over-limit measurements in the same episode do not emit")
    void repeatedViolationSuppressed() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        assertTrue(m.onOffsetMeasured(150));
        assertFalse(m.onOffsetMeasured(200), "same episode must not re-emit");
        assertFalse(m.onOffsetMeasured(5_000), "a still-stuck clock must not spam rows");
        assertTrue(m.isViolated());
    }

    @Test
    @DisplayName("recovery resets the episode so a later violation emits again")
    void recoveryResetsEpisode() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        assertTrue(m.onOffsetMeasured(150));
        assertFalse(m.onOffsetMeasured(101), "still violated — suppressed");
        assertFalse(m.onOffsetMeasured(100), "at the limit is NOT a violation (boundary)");
        assertFalse(m.isViolated(), "within limit clears the violation");
        assertTrue(m.onOffsetMeasured(-150), "a new episode must emit again");
        assertFalse(m.onOffsetMeasured(-99), "recovered — suppressed");
    }

    @Test
    @DisplayName("boundary: abs(offset) == limit is not a violation, limit+1 is")
    void boundaryAtLimit() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        assertFalse(m.onOffsetMeasured(LIMIT), "abs == limit must pass (mirrors NtpClockChecker)");
        assertFalse(m.onOffsetMeasured(-LIMIT), "negative abs == limit must pass");
        assertTrue(m.onOffsetMeasured(LIMIT + 1), "abs == limit+1 must violate");
        assertFalse(m.onOffsetMeasured(0), "recovery clears the episode");
        assertTrue(m.onOffsetMeasured(-(LIMIT + 1)), "clock behind by limit+1 must violate (new episode after recovery)");
    }

    @Test
    @DisplayName("negative offsets (clock behind) count as violations when beyond the limit")
    void negativeOffsetViolates() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        assertTrue(m.onOffsetMeasured(-200), "clock behind beyond the limit must emit");
        assertFalse(m.onOffsetMeasured(-150), "same episode suppressed");
        assertFalse(m.onOffsetMeasured(0), "perfect sync clears the episode");
    }

    @Test
    @DisplayName("zero and within-limit offsets never emit")
    void withinLimitNeverEmits() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        assertFalse(m.onOffsetMeasured(0));
        assertFalse(m.onOffsetMeasured(50));
        assertFalse(m.onOffsetMeasured(-50));
        assertFalse(m.isViolated());
    }

    @Test
    @DisplayName("non-positive limit is rejected at construction")
    void rejectsNonPositiveLimit() {
        assertThrows(IllegalArgumentException.class, () -> new TimeJumpMonitor(0));
        assertThrows(IllegalArgumentException.class, () -> new TimeJumpMonitor(-1));
    }

    @Test
    @DisplayName("exactly one emission across a long stuck-clock sequence")
    void oneEmissionPerEpisodeAcrossSequence() {
        TimeJumpMonitor m = new TimeJumpMonitor(LIMIT);
        int emissions = 0;
        long[] sequence = {120, 130, 125, 500, 900, 140, 120, 101}; // all over 100
        for (long offset : sequence) {
            if (m.onOffsetMeasured(offset)) emissions++;
        }
        assertEquals(1, emissions, "a stuck clock must produce exactly one TIME_JUMP row");
    }
}
