package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Canonical-signal identity policy (DEC-035, tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001). */
@DisplayName("CanonicalSignalPolicy")
class CanonicalSignalPolicyTest {

    private static final String EXPECTED_SCHEMA = SignalCandidatesTableColumns.SCHEMA_VERSION_V2;
    private static final String EXPECTED_STRATEGY = SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID;
    private static final String EXPECTED_VERSION = SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION;
    private static final String EXPECTED_RULE = SignalCandidatesTableColumns.CANONICAL_RULE_ID;

    @Test
    @DisplayName("the pinned canonical identity matches itself")
    void pinnedCanonicalIdentityMatches() {
        assertTrue(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE));
    }

    @Test
    @DisplayName("any of the four identity columns drifting from the expected value is non-canonical")
    void anyDriftingIdentityColumnIsNonCanonical() {
        assertFalse(CanonicalSignalPolicy.isCanonical(
                "1", EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "schema_version drift must be non-canonical");
        assertFalse(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, "other-strategy", EXPECTED_VERSION, EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "strategy_id drift must be non-canonical");
        assertFalse(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, "9.9.9", EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "strategy_version drift must be non-canonical");
        assertFalse(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, "other-rule",
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "rule_id drift must be non-canonical");
    }

    @Test
    @DisplayName("null and blank values never match (strict exact-match semantics)")
    void nullAndBlankValuesNeverMatch() {
        assertFalse(CanonicalSignalPolicy.isCanonical(
                null, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "null schema_version must be non-canonical");
        assertFalse(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, "", EXPECTED_VERSION, EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "blank strategy_id must be non-canonical");
        assertFalse(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, " " + EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE,
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE),
                "whitespace is not trimmed — a padded value is a different identity");
    }

    @Test
    @DisplayName("a null expected value fails closed (never matches)")
    void nullExpectedValueFailsClosed() {
        assertFalse(CanonicalSignalPolicy.isCanonical(
                EXPECTED_SCHEMA, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE,
                null, EXPECTED_STRATEGY, EXPECTED_VERSION, EXPECTED_RULE));
    }
}
