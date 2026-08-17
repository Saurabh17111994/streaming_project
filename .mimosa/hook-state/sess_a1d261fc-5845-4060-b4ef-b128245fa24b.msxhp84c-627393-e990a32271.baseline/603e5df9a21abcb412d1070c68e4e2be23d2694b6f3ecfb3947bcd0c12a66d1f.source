package com.trading.ingestion.discontinuity;

/**
 * ING-FAIL-007: decides when a {@code TIME_JUMP} discontinuity must be
 * emitted from a periodic clock-offset measurement.
 *
 * <p>The service re-measures the NTP clock offset on an interval. A measured
 * offset whose absolute value exceeds {@code CLOCK_OFFSET_LIMIT_MS} is a
 * clock-jump violation. This monitor emits at most ONE {@code TIME_JUMP} row
 * per violation episode: the first over-limit measurement after a
 * within-limit measurement (or after construction). While the offset stays
 * over the limit, subsequent measurements are suppressed so a stuck clock
 * cannot spam discontinuity rows; the first within-limit measurement resets
 * the episode.
 *
 * <p>Boundary: {@code abs(offset) > limit} is a violation, {@code abs(offset)
 * == limit} is not — this mirrors {@code NtpClockChecker} ({@code
 * Math.abs(offset) <= offsetLimitMs} passes).
 *
 * <p>Thread-safety: a single {@link #onOffsetMeasured} caller (the service's
 * clock-monitor scheduler thread) is expected; the class is not synchronized
 * for concurrent callers.
 */
public final class TimeJumpMonitor {

    private final long limitMs;
    private boolean violated;

    public TimeJumpMonitor(long limitMs) {
        if (limitMs <= 0) {
            throw new IllegalArgumentException("limitMs must be positive, got " + limitMs);
        }
        this.limitMs = limitMs;
    }

    /**
     * Record one clock-offset measurement.
     *
     * @param offsetMs signed measured offset (positive = local clock ahead)
     * @return {@code true} exactly once per violation episode — the caller
     *         should write a {@code TIME_JUMP} discontinuity row for this
     *         measurement and for no other
     */
    public boolean onOffsetMeasured(long offsetMs) {
        boolean over = Math.abs(offsetMs) > limitMs;
        boolean emit = over && !violated;
        violated = over;
        return emit;
    }

    /** Whether the monitor currently considers the clock in violation (no emission side effect). */
    public boolean isViolated() {
        return violated;
    }
}
