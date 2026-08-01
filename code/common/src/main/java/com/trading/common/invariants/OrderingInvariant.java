package com.trading.common.invariants;

import java.util.Comparator;

/**
 * Ordering invariant (docs/08_implementation/01-foundation.md &rarr; "Ordering invariant", orig L645).
 *
 * <p>Event-time ordering per instrument, with a deterministic fingerprint tie-break so that any
 * replay or re-read produces the same sequence.
 */
public final class OrderingInvariant {

    private OrderingInvariant() {}

    public static final Comparator<Record> BY_INSTRUMENT_EVENT_TIME =
        Comparator.comparingLong((Record r) -> r.instrumentToken)
            .thenComparingLong(r -> r.eventTime)
            .thenComparing(r -> r.eventFingerprint); // deterministic tie-break

    public static final class Record {
        public final long instrumentToken;
        public final long eventTime;
        public final String eventFingerprint;

        public Record(long instrumentToken, long eventTime, String eventFingerprint) {
            this.instrumentToken = instrumentToken;
            this.eventTime = eventTime;
            this.eventFingerprint = eventFingerprint;
        }
    }
}
