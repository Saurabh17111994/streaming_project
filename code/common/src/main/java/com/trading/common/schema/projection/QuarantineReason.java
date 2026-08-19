package com.trading.common.schema.projection;

/**
 * Disposition reasons for the Postback_Quarantine LOG (16_postback_quarantine.sql
 * reason vocabulary). T6 (CHG-045) adds the fingerprint/lifecycle reasons; the
 * DDL correlation reasons are preserved verbatim so the vocabulary maps 1:1.
 */
public enum QuarantineReason {
    MISSING_BROKER_ID,
    AMBIGUOUS_CORRELATION,
    NO_MATCHING_INSTRUCTION,
    UNPARSEABLE_PAYLOAD,
    UNKNOWN_POSTBACK_TYPE,
    DUP_FINGERPRINT,
    FINGERPRINT_MISMATCH,
    LIFECYCLE_CONFLICT,
    TERMINAL_REGRESSION,
    IMPOSSIBLE_QUANTITY,
    UNKNOWN_STATUS,
    STALE_EVENT,
    FILL_OVERRUN,
    POSITION_VIOLATION
}
