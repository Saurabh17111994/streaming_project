package com.trading.common.model;

import java.util.EnumSet;
import java.util.Set;

/**
 * Executor gate state machine: HALTED → RECONCILING → APPROVAL_PENDING → ENABLED.
 * On uncertain state, any state → HALTED.
 * See REQ-EXE-002.
 *
 * <p>This enum is the <b>single source of truth</b> for the legal gate-state
 * matrix (T5 reconciliation, CHG-044). The canonical lifecycle from
 * docs/08_implementation/05-execution-core.md ("State machines &rarr; Gate"):
 * {@code HALTED → RECONCILING → APPROVAL_PENDING → ENABLED → HALTED} — the only
 * enablement path is the three forward steps, and a safety halt returns to
 * HALTED from any state. No jump skips a step (HALTED &rarr; APPROVAL_PENDING is
 * illegal) and no sanctioned backward step exists other than the halt
 * (APPROVAL_PENDING &rarr; RECONCILING is illegal — an approval failure halts
 * rather than silently re-reconciling). This matches the Rust gate's
 * sanctioned-transition check in {@code code/02_services/04_executor/src/gate.rs}.
 */
public enum GateState {
    HALTED,
    RECONCILING,
    APPROVAL_PENDING,
    ENABLED;

    /**
     * The canonical legal target set for this state: the sanctioned forward
     * step plus the safety halt to HALTED. Every accepted transition increments
     * the gate epoch by exactly 1 (the caller owns the epoch bookkeeping).
     */
    public Set<GateState> legalTargets() {
        return switch (this) {
            case HALTED -> EnumSet.of(RECONCILING);
            case RECONCILING -> EnumSet.of(APPROVAL_PENDING, HALTED);
            case APPROVAL_PENDING -> EnumSet.of(ENABLED, HALTED);
            case ENABLED -> EnumSet.of(HALTED);
        };
    }
}
