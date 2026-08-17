package com.trading.common.invariants;

/**
 * Delivery semantics boundary
 * (docs/08_implementation/01-foundation.md &rarr; "Delivery invariant", orig L632).
 *
 * <p>Exactly-once holds only within the tested Flink/Fluss boundary. Every external side effect
 * (broker order, fill capture) is at-least-once and reconciled.
 */
public enum DeliverySemantics {
    EXACTLY_ONCE_WITHIN_TESTED_BOUNDARY,
    AT_LEAST_ONCE_EXTERNAL_BROKER
}
