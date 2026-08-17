package com.trading.ingestion.model;

/** Packet validity classification — Compute uses this to exclude invalid ticks from candles. */
public enum ValidityClassification {
    /** Normal accepted trade tick; eligible for candle aggregation. */
    VALID_TRADE,
    /** Quote, depth, or other non-trade event; preserved but excluded from candles. */
    VALID_NON_TRADE,
    /** Trade values out of range; raw bytes preserved, typed fields present, flagged. */
    INVALID_VALUES,
    /** Event time missing or unverifiable; quarantined from candle path. */
    INVALID_TIMESTAMP,
    /** Missing instrument identity; not appended to keyed raw table. */
    MISSING_INSTRUMENT,
    /** Decode failure; original bytes preserved in quarantine. */
    DECODE_FAILURE,
    /** Unknown protocol version; quarantined. */
    UNKNOWN_VERSION
}
