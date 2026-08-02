package com.trading.common.observability;

/**
 * Durable, idempotent safety-halt request
 * (docs/08_implementation/01-foundation.md &rarr; "Observability invariant" L727 / "Order safety invariant" L699).
 */
public final class SafetyHaltRequest {

    private SafetyHaltRequest() {}

    public enum Result {
        PENDING,
        APPLIED,
        ALREADY_HALTED,
        REJECTED_STALE_EPOCH,
        REJECTED_SCOPE_MISMATCH,
        FAILED
    }

    /**
     * True only for a repeated terminal-success result (R-129). The old
     * implementation returned true for ANY repeated result — treating repeated
     * FAILED (the halt was never applied) and PENDING (non-terminal) as
     * idempotent no-ops, which would silently swallow a halt that still needs
     * to be applied.
     */
    public static boolean isIdempotentDuplicate(Result prior, Result incoming) {
        if (prior == null || prior != incoming) {
            return false;
        }
        // Only terminal-success results are idempotent.
        return prior == Result.APPLIED || prior == Result.ALREADY_HALTED;
    }

    /** No auto-resume: once halted, only an explicit, authorized UNHALT may clear it. */
    public static boolean permitsAutoResume() {
        return false;
    }
}
