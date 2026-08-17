package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Canonical-candle policy decision matrix (CANDLE-KV-REPLAY-001 A2.3).
 *
 * <p>The KV projection and the migration audit must agree on exactly which
 * rows are canonical: both version columns equal the pinned expected values,
 * exact match, nulls and blanks never match.
 */
@DisplayName("CanonicalCandlePolicy: exact version match, nulls and blanks never canonical")
class CanonicalCandlePolicyTest {

    private static final String ALGO = "candle-15s-v1";
    private static final String CONFIG = "1.0.0";

    @Test
    @DisplayName("exact match of both pinned versions is canonical")
    void exactMatchIsCanonical() {
        assertTrue(CanonicalCandlePolicy.isCanonical(ALGO, CONFIG, ALGO, CONFIG));
    }

    @Test
    @DisplayName("algorithm version drift is not canonical")
    void algorithmDriftIsNotCanonical() {
        assertFalse(CanonicalCandlePolicy.isCanonical("candle-15s-v2", CONFIG, ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical("candle-20s-v1", CONFIG, ALGO, CONFIG));
    }

    @Test
    @DisplayName("configuration version drift is not canonical")
    void configurationDriftIsNotCanonical() {
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, "1.1.0", ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, "1.0.0-dev", ALGO, CONFIG));
    }

    @Test
    @DisplayName("both versions drifted is not canonical")
    void bothDriftedIsNotCanonical() {
        assertFalse(CanonicalCandlePolicy.isCanonical("candle-15s-v2", "1.1.0", ALGO, CONFIG));
    }

    @Test
    @DisplayName("null or blank row versions are never canonical")
    void nullOrBlankRowVersionsAreNotCanonical() {
        assertFalse(CanonicalCandlePolicy.isCanonical(null, CONFIG, ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, null, ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical(null, null, ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical("", CONFIG, ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, "", ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical("  ", CONFIG, ALGO, CONFIG));
    }

    @Test
    @DisplayName("null or blank expected values never match (no NPE)")
    void nullOrBlankExpectedNeverMatch() {
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, CONFIG, null, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, CONFIG, ALGO, null));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, CONFIG, null, null));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, CONFIG, "", CONFIG));
    }

    @Test
    @DisplayName("whitespace padding is a different version, not an equivalent one")
    void whitespacePaddingIsNotEquivalent() {
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO + " ", CONFIG, ALGO, CONFIG));
        assertFalse(CanonicalCandlePolicy.isCanonical(ALGO, " " + CONFIG, ALGO, CONFIG));
    }
}
