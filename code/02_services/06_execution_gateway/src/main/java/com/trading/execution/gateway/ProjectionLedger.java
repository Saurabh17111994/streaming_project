package com.trading.execution.gateway;

import java.util.Map;

/** Durable projection workflow states. Cross-table writes are deliberately not atomic. */
public final class ProjectionLedger {
    public enum State { RECEIVED, AUDIT_WRITTEN, LIFECYCLE_APPLIED,
        POSITION_APPLIED_OR_NOT_REQUIRED, COMPLETE }
    private static final Map<State, State> NEXT = Map.of(
            State.RECEIVED, State.AUDIT_WRITTEN,
            State.AUDIT_WRITTEN, State.LIFECYCLE_APPLIED,
            State.LIFECYCLE_APPLIED, State.POSITION_APPLIED_OR_NOT_REQUIRED,
            State.POSITION_APPLIED_OR_NOT_REQUIRED, State.COMPLETE);
    private ProjectionLedger() {}

    public static State advance(State current, State requested) {
        if (current == requested) return current;
        if (NEXT.get(current) != requested) throw new IllegalStateException(
                "invalid ledger transition " + current + " -> " + requested);
        return requested;
    }
    public static boolean terminal(State state) { return state == State.COMPLETE; }
    public static boolean recoverable(State state) { return !terminal(state); }
}
