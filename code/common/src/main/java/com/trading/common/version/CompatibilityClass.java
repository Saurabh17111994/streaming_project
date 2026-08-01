package com.trading.common.version;

/**
 * Compatibility classification applied per integration boundary
 * (docs/08_implementation/01-foundation.md &rarr; "Compatibility classifications", orig L302).
 */
public enum CompatibilityClass {
    /** Proven correct for the stated scenario on the pinned versions. */
    COMPATIBLE,
    /** Works within documented limits; behavior outside those limits is undefined. */
    LIMITED,
    /** Proven incorrect / unsafe for the stated scenario. */
    INCOMPATIBLE,
    /** Not yet proven; treated as unavailable until evidence exists. */
    UNKNOWN,
    /** Classification not applicable to this boundary. */
    NA
}
