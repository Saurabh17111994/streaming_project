package com.trading.common.schema;

/**
 * EOD controller state machine + offload gate
 * (docs/08_implementation/01-foundation.md &rarr; "EOD controller and offload gate", orig L511).
 *
 * <p>State machine:
 * <pre>{@code
 * PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED
 *                     ↘ FAILED_RETRYABLE
 *                     ↘ FAILED_MANUAL
 * }</pre>
 *
 * <p>Source data for a trading day SHALL not expire while the manifest is unverified,
 * retryable, or under reconciliation. Source data cannot expire unless state is
 * {@code VERIFIED}, and at least three complete trading days remain live.
 */
public enum EodControllerState {
    PENDING,             // waiting for end-of-day boundary
    WRITING,             // writing manifest + data to lake
    COMMITTED,           // lake commit succeeded; reconciliation pending
    VERIFYING,           // lake read-back / reconciliation in progress
    VERIFIED,            // safe; source retention may expire
    FAILED_RETRYABLE,    // offload/verify failed; retry with backoff (extends retention)
    FAILED_MANUAL;       // offload/verify failed; requires manual reconciliation

    /** Source retention may only expire once VERIFIED. */
    public boolean permitsSourceExpiry() {
        return this == VERIFIED;
    }

    /** Unverified or retryable states require retention extension. */
    public boolean requiresRetentionExtension() {
        return this == PENDING || this == WRITING || this == COMMITTED
                || this == VERIFYING || this == FAILED_RETRYABLE || this == FAILED_MANUAL;
    }

    /** Retryable failure — the controller should retry with backoff. */
    public boolean isRetryable() {
        return this == FAILED_RETRYABLE;
    }
}
