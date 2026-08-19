package com.trading.common.model;

/**
 * Runtime validator for {@link GateState} and {@link AttemptPhase} transitions.
 *
 * <p>Every transition is checked against a legal matrix. Illegal transitions
 * are rejected and produce an auditable error. The contract:
 *
 * <ul>
 *   <li>Gate transitions compare current epoch/phase before proceeding</li>
 *   <li>Every transition is auditable (result carries a reason)</li>
 *   <li>Uncertain outcomes transition to {@link GateState#HALTED}</li>
 *   <li>Stale-epoch requests are rejected without side-effects</li>
 * </ul>
 *
 * <p>The legal matrices are not defined here: {@link AttemptPhase#legalTargets}
 * and {@link GateState#legalTargets} are the single source of truth (T5
 * reconciliation, CHG-044). The one canonical table is shared by this validator,
 * the attempt-store writer, and the Rust gate, so no runtime task can start
 * while two different meanings exist for the same transition.
 *
 * <p>Corrected vs the pre-T5 matrix: ACCEPTED &rarr; REJECTED is now rejected
 * (ACCEPTED is terminal per the attempt-store's TERMINAL_PHASES); terminal
 * phases can no longer become UNKNOWN; PREPARED can no longer go to CANCELLED
 * or UNKNOWN (its only exit is SUBMITTING); HALTED &rarr; APPROVAL_PENDING and
 * APPROVAL_PENDING &rarr; RECONCILING are rejected (they skip the only
 * enablement path / silently re-reconcile instead of halting).
 *
 * <p>Source: docs/08_implementation/05-execution-core.md &rarr; "State machines"
 * and "Attempt protocol"; docs/08_implementation/01-foundation.md &rarr;
 * "Order safety invariant" (orig L699).
 */
public final class GateTransitionValidator {

    private GateTransitionValidator() {}

    /**
     * Validate a gate-state transition.
     *
     * @param currentState the gate's current state (never null)
     * @param targetState  the desired target state (never null)
     * @param currentEpoch the gate's current epoch
     * @param requestEpoch the epoch of the transition request
     * @return the validation result
     */
    public static GateResult validateGateTransition(
            GateState currentState,
            GateState targetState,
            long currentEpoch,
            long requestEpoch) {

        // 1. Stale-epoch rejection — the request is for an old generation.
        // R-076: the audit trail must record the ACTUAL current state as the
        // from-state — the old code reported HALTED, which would make an
        // operator believe the gate was already halted when it may have been
        // ENABLED.
        if (requestEpoch < currentEpoch) {
            return GateResult.rejected(currentState, targetState,
                    "stale epoch: request=" + requestEpoch + " < current=" + currentEpoch
                            + "; epoch mismatch may indicate a lost lease or delayed message");
        }

        // 2. Same-state idempotent — no-op
        if (currentState == targetState) {
            return GateResult.allowed(currentState, targetState,
                    "idempotent: already in " + targetState);
        }

        // 3. Check legal transition against the canonical matrix.
        boolean legal = currentState.legalTargets().contains(targetState);
        if (!legal) {
            return GateResult.rejected(currentState, targetState,
                    "illegal transition: " + currentState + " → " + targetState);
        }

        // 4. Forward-epoch gap (requestEpoch > currentEpoch)
        // Allowed but logged — the caller owns epoch management.
        // The executor fencing lease must still be valid.

        return GateResult.allowed(currentState, targetState,
                "legal transition: " + currentState + " → " + targetState
                        + (requestEpoch > currentEpoch
                            ? " (forward epoch " + requestEpoch + ")"
                            : ""));
    }

    /**
     * Validate an attempt-phase transition.
     *
     * <p>Every attempt transition must be auditable. Only SUBMITTING can become
     * UNKNOWN (network failure, timeout, crash); terminal phases never become
     * UNKNOWN. UNKNOWN &rarr; ACCEPTED / REJECTED / CANCELLED is legal in the
     * full matrix but is <b>reconciliation-only</b> — callers must route it
     * through {@code resolveUnknown}, never through the submission path
     * (see {@link #isReconciliationOnly}).
     *
     * @param currentPhase the attempt's current phase
     * @param targetPhase  the desired target phase
     * @param reason       human-readable reason (for audit trail)
     * @return the validation result
     */
    public static AttemptResult validateAttemptTransition(
            AttemptPhase currentPhase,
            AttemptPhase targetPhase,
            String reason) {

        // Same-phase — no-op (idempotent)
        if (currentPhase == targetPhase) {
            return AttemptResult.allowed(currentPhase, targetPhase, reason,
                    "idempotent: already in " + targetPhase);
        }

        boolean legal = AttemptPhase.isLegal(currentPhase, targetPhase);
        if (!legal) {
            return AttemptResult.rejected(currentPhase, targetPhase, reason,
                    "illegal transition: " + currentPhase + " → " + targetPhase);
        }

        return AttemptResult.allowed(currentPhase, targetPhase, reason,
                "legal transition: " + currentPhase + " → " + targetPhase);
    }

    /**
     * Whether {@code from -> to} is legal <b>only</b> through the explicit
     * reconciliation path (UNKNOWN &rarr; terminal). Callers that must separate
     * the submission path from reconciliation (the attempt-store writer, the
     * durable command gate) use this to route UNKNOWN exits through
     * {@code resolveUnknown} instead of the submission path.
     */
    public static boolean isReconciliationOnly(AttemptPhase from, AttemptPhase to) {
        return from != null && from.isReconciliationSource()
                && from.legalTargets().contains(to);
    }

    /** Canonical terminal-phase check (ACCEPTED / REJECTED / CANCELLED). */
    public static boolean isTerminal(AttemptPhase phase) {
        return phase != null && phase.isTerminal();
    }

    // ---- internal helpers ----

    /** Whether the gate has a sanctioned non-idempotent road to the target. */
    static boolean isLegalGateTransition(GateState from, GateState to) {
        return from != null && from.legalTargets().contains(to);
    }

    // ---- result types ----

    /** Outcome of a gate-state transition validation. */
    public record GateResult(
            boolean allowed,
            GateState from,
            GateState to,
            String detail) {

        public static GateResult allowed(GateState from, GateState to, String detail) {
            return new GateResult(true, from, to, detail);
        }

        public static GateResult rejected(GateState from, GateState to, String detail) {
            return new GateResult(false, from, to, detail);
        }

        /** Whether a rejected gate transition means the process should halt entirely. */
        public boolean requiresHalt() {
            return !allowed && (from == GateState.ENABLED && to != GateState.HALTED);
        }
    }

    /** Outcome of an attempt-phase transition validation. */
    public record AttemptResult(
            boolean allowed,
            AttemptPhase from,
            AttemptPhase to,
            String reason,
            String detail) {

        public static AttemptResult allowed(AttemptPhase from, AttemptPhase to,
                                            String reason, String detail) {
            return new AttemptResult(true, from, to, reason, detail);
        }

        public static AttemptResult rejected(AttemptPhase from, AttemptPhase to,
                                             String reason, String detail) {
            return new AttemptResult(false, from, to, reason, detail);
        }

        /** UNKNOWN transitions are always legally valid. */
        public boolean isUncertain() {
            return allowed && to == AttemptPhase.UNKNOWN;
        }
    }
}
