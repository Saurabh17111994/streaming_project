package com.trading.common.model;

/**
 * Executor gate state machine: HALTED → RECONCILING → APPROVAL_PENDING → ENABLED.
 * On uncertain state, any state → HALTED.
 * See REQ-EXE-002.
 */
public enum GateState {
    HALTED,
    RECONCILING,
    APPROVAL_PENDING,
    ENABLED
}
