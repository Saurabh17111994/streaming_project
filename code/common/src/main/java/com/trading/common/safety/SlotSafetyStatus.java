package com.trading.common.safety;

/**
 * Lifecycle status of a slot-scoped safety request
 * (plan.md &sect; "Slot-scoped safety propagation"; mirrors the
 * {@code SafetyState} vocabulary written by the ingestion
 * {@code SafetyHaltWriter}: {@code UNSAFE} / {@code RECOVERED}).
 */
public enum SlotSafetyStatus {
    UNSAFE,
    RECOVERED
}
