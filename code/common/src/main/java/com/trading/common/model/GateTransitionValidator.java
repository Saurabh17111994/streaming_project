package com.trading.common.model;

import java.util.EnumSet;
import java.util.Set;

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
 * <p>Source: docs/08_implementation/01-foundation.md &rarr;
 * "Table categories and invariants" (L434) + "Order safety invariant" (L699).
 */
public final class GateTransitionValidator {

    private GateTransitionValidator() {}

    // ---- GateState transitions ----

    /** Legal source states for each gate target state. */
    private static final Set<GateState> LEGAL_TO_HALTED = EnumSet.of(
            GateState.RECONCILING, GateState.APPROVAL_PENDING, GateState.ENABLED);
    private static final Set<GateState> LEGAL_TO_RECONCILING = EnumSet.of(
            GateState.HALTED, GateState.APPROVAL_PENDING);
    private static final Set<GateState> LEGAL_TO_APPROVAL_PENDING = EnumSet.of(
            GateState.RECONCILING, GateState.HALTED);
    private static final Set<GateState> LEGAL_TO_ENABLED = EnumSet.of(
            GateState.APPROVAL_PENDING);

    // ---- AttemptPhase transitions ----

    private static final Set<AttemptPhase> LEGAL_TO_SUBMITTING = EnumSet.of(
            AttemptPhase.PREPARED);
    private static final Set<AttemptPhase> LEGAL_TO_ACCEPTED = EnumSet.of(
            AttemptPhase.SUBMITTING);
    private static final Set<AttemptPhase> LEGAL_TO_REJECTED = EnumSet.of(
            AttemptPhase.SUBMITTING, AttemptPhase.ACCEPTED);
    private static final Set<AttemptPhase> LEGAL_TO_CANCELLED = EnumSet.of(
            AttemptPhase.PREPARED, AttemptPhase.SUBMITTING);
    /** Any phase can transition to UNKNOWN (network failure, timeout, crash). */
    private static final Set<AttemptPhase> LEGAL_TO_UNKNOWN = EnumSet.allOf(AttemptPhase.class);

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

        // 1. Stale-epoch rejection — the request is for an old generation
        if (requestEpoch < currentEpoch) {
            return GateResult.rejected(GateState.HALTED, targetState,
                    "stale epoch: request=" + requestEpoch + " < current=" + currentEpoch
                            + "; epoch mismatch may indicate a lost lease or delayed message");
        }

        // 2. Same-state idempotent — no-op
        if (currentState == targetState) {
            return GateResult.allowed(currentState, targetState,
                    "idempotent: already in " + targetState);
        }

        // 3. Check legal transition
        boolean legal = isLegalGateTransition(currentState, targetState);
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
     * <p>Every attempt transition must be auditable. UNKNOWN transitions are
     * always legal (any phase can become UNKNOWN on network/crash failure)
     * but the caller must record the reason.
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

        boolean legal = isLegalAttemptTransition(currentPhase, targetPhase);
        if (!legal) {
            return AttemptResult.rejected(currentPhase, targetPhase, reason,
                    "illegal transition: " + currentPhase + " → " + targetPhase);
        }

        return AttemptResult.allowed(currentPhase, targetPhase, reason,
                "legal transition: " + currentPhase + " → " + targetPhase);
    }

    // ---- internal helpers ----

    private static boolean isLegalGateTransition(GateState from, GateState to) {
        Set<GateState> legalSources = switch (to) {
            case HALTED -> LEGAL_TO_HALTED;
            case RECONCILING -> LEGAL_TO_RECONCILING;
            case APPROVAL_PENDING -> LEGAL_TO_APPROVAL_PENDING;
            case ENABLED -> LEGAL_TO_ENABLED;
        };
        return legalSources.contains(from);
    }

    private static boolean isLegalAttemptTransition(AttemptPhase from, AttemptPhase to) {
        Set<AttemptPhase> legalSources = switch (to) {
            case SUBMITTING -> LEGAL_TO_SUBMITTING;
            case ACCEPTED -> LEGAL_TO_ACCEPTED;
            case REJECTED -> LEGAL_TO_REJECTED;
            case UNKNOWN -> LEGAL_TO_UNKNOWN;
            case CANCELLED -> LEGAL_TO_CANCELLED;
            default -> Set.of(from); // PREPARED has no incoming transitions defined
        };
        return legalSources.contains(from);
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
