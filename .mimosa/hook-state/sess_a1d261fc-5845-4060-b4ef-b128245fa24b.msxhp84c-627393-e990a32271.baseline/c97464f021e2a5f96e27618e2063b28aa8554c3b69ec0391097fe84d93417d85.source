package com.trading.common.invariants;

/**
 * Time invariant (docs/08_implementation/01-foundation.md &rarr; "Time invariant", orig L654).
 *
 * <p>Canonical timestamp fields and a monotonic (non-negative duration) clock. All timestamps are
 * UTC epoch millis.
 */
public final class TimeInvariant {

    private TimeInvariant() {}

    public static final String EVENT_TIME = "event_time";
    public static final String RECEIVE_TIME = "receive_time";
    public static final String PERSIST_START = "persist_start";
    public static final String PERSIST_ACK = "persist_ack";
    public static final String PROCESSING_TIME = "processing_time";
    public static final String SCHEMA_VERSION = "schema_version";

    /** Duration must be non-negative (monotonic clock). */
    public static boolean isMonotonic(long start, long end) {
        return end >= start;
    }

    /** All timestamps are positive UTC epoch millis. */
    public static boolean isUtcEpochMillis(long ts) {
        return ts > 0;
    }
}
