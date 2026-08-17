package com.trading.common.observability;

/**
 * Alert thresholds (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 *
 * <p>Each alert fires only after a {@link #CONSECUTIVE_BREACH_SECONDS}-second consecutive breach.
 * The safety halt is idempotent and never auto-resumes (handled by {@link SafetyHaltRequest}).
 */
public final class AlertThresholds {

    private AlertThresholds() {}

    public static final int CONSECUTIVE_BREACH_SECONDS = 60;

    public enum Alert {
        CONTAINER_MEMORY("container_memory_pct >= 85"),
        PENDING_APPEND_RECORDS("pending_append_records >= 80% of limit"),
        PENDING_APPEND_BYTES("pending_append_bytes >= 80% of limit"),
        CHECKPOINT_DURATION("checkpoint duration > timeout"),
        CHECKPOINT_FAILURE_RATE("checkpoint failure rate above threshold"),
        CHANGELOG_GAP("detected changelog gap"),
        MISSING_FILL("postback fill missing beyond SLA"),
        STALE_SIGNAL("signal staleness beyond bound");

        public final String condition;

        Alert(String condition) {
            this.condition = condition;
        }
    }
}
