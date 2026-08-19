package com.trading.common.model;

import java.util.EnumSet;
import java.util.Set;

/**
 * Execution attempt phases. See REQ-EXE-005.
 *
 * <p>This enum is the <b>single source of truth</b> for the legal attempt
 * phase matrix (T5 reconciliation, CHG-044). The canonical rules, from the
 * dossier "Attempt rules" (docs/08_implementation/05-execution-core.md) and
 * mirrored by {@link com.trading.common.schema.execution.InMemoryAttemptStore}:
 * <ul>
 *   <li>PREPARED &rarr; SUBMITTING (the only exit from PREPARED);</li>
 *   <li>SUBMITTING &rarr; ACCEPTED / REJECTED / CANCELLED / UNKNOWN
 *       (submission path);</li>
 *   <li>UNKNOWN &rarr; ACCEPTED / REJECTED / CANCELLED — reconciliation-only
 *       exits, never through the submission path (no auto-retry, DEC-011,
 *       DEC-030);</li>
 *   <li>ACCEPTED / REJECTED / CANCELLED are <b>terminal</b> — no outgoing
 *       transition of any kind, including to UNKNOWN (a terminal outcome never
 *       becomes uncertain again).</li>
 * </ul>
 *
 * <p>Previously {@link GateTransitionValidator} hard-coded a matrix that
 * disagreed with the store: it allowed ACCEPTED &rarr; REJECTED and terminal
 * &rarr; UNKNOWN and PREPARED &rarr; CANCELLED/UNKNOWN. Those are corrected
 * here so the validator and the store can never drift again (the validator
 * delegates to {@link #legalTargets}).
 */
public enum AttemptPhase {
    PREPARED,
    SUBMITTING,
    ACCEPTED,
    REJECTED,
    UNKNOWN,
    CANCELLED;

    /** Phase whose only exits are explicit reconciliation results. */
    private static final Set<AttemptPhase> RECONCILIATION_SOURCES = EnumSet.of(UNKNOWN);

    /** Terminal phases: a terminal outcome is final and never re-opened. */
    private static final Set<AttemptPhase> TERMINAL = EnumSet.of(ACCEPTED, REJECTED, CANCELLED);

    /**
     * The canonical legal target set for this phase — the complete matrix
     * including the reconciliation-only exits of {@link #UNKNOWN}. Used by
     * {@link GateTransitionValidator} and by the attempt-store writer so a
     * single table defines what is legal everywhere.
     */
    public Set<AttemptPhase> legalTargets() {
        return switch (this) {
            case PREPARED -> EnumSet.of(SUBMITTING);
            case SUBMITTING -> EnumSet.of(ACCEPTED, REJECTED, CANCELLED, UNKNOWN);
            case UNKNOWN -> EnumSet.of(ACCEPTED, REJECTED, CANCELLED);
            case ACCEPTED, REJECTED, CANCELLED -> EnumSet.noneOf(AttemptPhase.class);
        };
    }

    /** Whether this phase can only be exited through explicit reconciliation. */
    public boolean isReconciliationSource() {
        return RECONCILIATION_SOURCES.contains(this);
    }

    /** Terminal phases can never transition again — even to UNKNOWN. */
    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }

    /** Canonical check: {@code from -> to} is in the full legal matrix. */
    public static boolean isLegal(AttemptPhase from, AttemptPhase to) {
        if (from == null || to == null) {
            return false;
        }
        return from != to && from.legalTargets().contains(to);
    }
}
