package com.trading.common.invariants;

/**
 * Failure invariant (docs/08_implementation/01-foundation.md &rarr; "Failure invariant", orig L684).
 *
 * <p>Defined behavior for each failure phase. Ambiguity is never guessed: it resolves to
 * {@link Disposition#UNKNOWN}, which forces quarantine + halt rather than a silent default.
 */
public final class FailureInvariant {

    private FailureInvariant() {}

    public enum Phase {
        BEFORE_ACK, DURING_PROCESSING, AFTER_ACK, TIMEOUT, DUPLICATE, RESTART, STALE, CORRUPT
    }

    public enum Disposition {
        RETRY, DROP, HALT, QUARANTINE, UNKNOWN
    }

    /** Ambiguity always resolves to UNKNOWN (no silent success). */
    public static Disposition onAmbiguity() {
        return Disposition.UNKNOWN;
    }
}
