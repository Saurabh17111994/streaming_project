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

    /** A repeat of the same result for the same halt_request_id is an idempotent no-op. */
    public static boolean isIdempotentDuplicate(Result prior, Result incoming) {
        return prior != null && prior == incoming;
    }

    /** No auto-resume: once halted, only an explicit, authorized UNHALT may clear it. */
    public static boolean permitsAutoResume() {
        return false;
    }
}
