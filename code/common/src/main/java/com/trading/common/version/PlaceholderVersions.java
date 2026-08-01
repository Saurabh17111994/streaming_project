package com.trading.common.version;

/**
 * Evidence-gated version placeholders
 * (docs/08_implementation/01-foundation.md &rarr; "Placeholder policy", orig L178).
 *
 * <p>Each MUST be replaced by a pinned, evidence-verified value before the corresponding
 * live-money path is enabled. They are intentionally NOT real versions, so a forgotten
 * substitution is obvious rather than silently wrong.
 */
public final class PlaceholderVersions {

    private PlaceholderVersions() {}

    public static final String BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED = "BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED";
    public static final String FLINK_VERSION_TO_BE_PINNED = "FLINK_VERSION_TO_BE_PINNED";
    public static final String FLUSS_VERSION_TO_BE_PINNED = "FLUSS_VERSION_TO_BE_PINNED";
    public static final String ARROW_API_CONTRACT_TO_BE_VERIFIED = "ARROW_API_CONTRACT_TO_BE_VERIFIED";
    public static final String OPENALGO_API_CONTRACT_TO_BE_VERIFIED = "OPENALGO_API_CONTRACT_TO_BE_VERIFIED";
    public static final String SCHEMA_LIFECYCLE_TO_BE_VERIFIED = "SCHEMA_LIFECYCLE_TO_BE_VERIFIED";

    /** A placeholder is any value that equals its own sentinel name. */
    public static boolean isPlaceholder(String value) {
        if (value == null) {
            return false;
        }
        return value.equals(BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED)
            || value.equals(FLINK_VERSION_TO_BE_PINNED)
            || value.equals(FLUSS_VERSION_TO_BE_PINNED)
            || value.equals(ARROW_API_CONTRACT_TO_BE_VERIFIED)
            || value.equals(OPENALGO_API_CONTRACT_TO_BE_VERIFIED)
            || value.equals(SCHEMA_LIFECYCLE_TO_BE_VERIFIED);
    }
}
