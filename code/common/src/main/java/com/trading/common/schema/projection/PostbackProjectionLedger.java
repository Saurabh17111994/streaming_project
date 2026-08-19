package com.trading.common.schema.projection;

/**
 * Durable projection workflow states for Postback_Projection_Ledger
 * (17_postback_projection_ledger.sql, keyed by {@code postback_event_id}).
 * Cross-table projection writes are deliberately not atomic, so this ledgo
 * is what makes independent Fluss writes restart-safe and idempotent: a row
 * re-read after a crash resumes from its recorded state, and a partial write
 * never reaches COMPLETE until every required table acknowledgement is seen.
 *
 * <p>The <b>order-side</b> states mirror the gateway {@code ProjectionLedger}
 * (RECEIVED &rarr; AUDIT_WRITTEN &rarr; LIFECYCLE_APPLIED &rarr;
 * POSITION_APPLIED_OR_NOT_REQUIRED &rarr; COMPLETE), with CORRELATED added as
 * the correlation gate and QUARANTINED/FAILED as terminal dispositions that
 * end the recovery walk. 05-execution-core.md: a partial write never becomes
 * COMPLETE until every required table acknowledgement is observed.
 */
public final class PostbackProjectionLedger {

    public enum State {
        RECEIVED,
        CORRELATED,
        AUDIT_WRITTEN,
        LIFECYCLE_APPLIED,
        POSITION_APPLIED_OR_NOT_REQUIRED,
        QUARANTINED,
        FAILED,
        COMPLETE
    }

    private PostbackProjectionLedger() {}

    public static State next(State current, State requested) {
        if (current == requested) {
            return current;
        }
        if (!isLegal(current, requested)) {
            throw new IllegalStateException("invalid ledger transition " + current
                    + " -> " + requested);
        }
        return requested;
    }

    public static boolean terminal(State state) {
        return state == State.COMPLETE || state == State.QUARANTINED || state == State.FAILED;
    }

    /** States from which a restarted projection must resume (not terminal). */
    public static boolean recoverable(State state) {
        return !terminal(state);
    }

    /** Whether the order-side table acknowledgements are all in. */
    public static boolean orderSideComplete(State state) {
        return state == State.COMPLETE || state == State.POSITION_APPLIED_OR_NOT_REQUIRED;
    }

    private static boolean isLegal(State current, State requested) {
        if (terminal(requested)) {
            // Any forward state may quarantine/fail/complete.
            return true;
        }
        return switch (current) {
            case RECEIVED -> requested == State.CORRELATED;
            case CORRELATED -> requested == State.AUDIT_WRITTEN;
            case AUDIT_WRITTEN -> requested == State.LIFECYCLE_APPLIED;
            case LIFECYCLE_APPLIED -> requested == State.POSITION_APPLIED_OR_NOT_REQUIRED;
            case POSITION_APPLIED_OR_NOT_REQUIRED -> false; // only terminal next
            default -> false; // terminal states cannot advance
        };
    }
}
