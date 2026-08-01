package com.trading.ingestion.fingerprint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-005: Fingerprint — same inputs → same hash; different inputs → different hash.
 */
@DisplayName("ING-UNIT-005: FingerprintBuilder")
class FingerprintBuilderTest {

    @Test
    @DisplayName("Same inputs produce identical fingerprints")
    void deterministicHash() {
        FingerprintBuilder.Result r1 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        FingerprintBuilder.Result r2 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        assertNotNull(r1.hash());
        assertNotNull(r2.hash());
        assertEquals(r1.hash(), r2.hash(), "Same inputs must produce same hash");
        assertEquals(1, r1.version());
        assertEquals(1, r2.version());
    }

    @Test
    @DisplayName("Different price → different fingerprint")
    void differentPriceDifferentHash() {
        FingerprintBuilder.Result r1 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        FingerprintBuilder.Result r2 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234600L, 50L, 234400L, 234600L); // price changed

        assertNotEquals(r1.hash(), r2.hash(), "Different price must produce different hash");
    }

    @Test
    @DisplayName("Different instrument → different fingerprint")
    void differentInstrumentDifferentHash() {
        FingerprintBuilder.Result r1 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        FingerprintBuilder.Result r2 = FingerprintBuilder.build(
                0L, 11536L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L); // instrument changed

        assertNotEquals(r1.hash(), r2.hash());
    }

    @Test
    @DisplayName("Different epoch → different fingerprint")
    void differentEpochDifferentHash() {
        FingerprintBuilder.Result r1 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        FingerprintBuilder.Result r2 = FingerprintBuilder.build(
                1L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L); // epoch changed

        assertNotEquals(r1.hash(), r2.hash());
    }

    @Test
    @DisplayName("Different tick type → different fingerprint")
    void differentTickTypeDifferentHash() {
        FingerprintBuilder.Result r1 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        FingerprintBuilder.Result r2 = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "QUOTE",
                234500L, 50L, 234400L, 234600L); // type changed

        assertNotEquals(r1.hash(), r2.hash());
    }

    @Test
    @DisplayName("Zero/nil values still produce valid hash")
    void zeroValuesProduceValidHash() {
        FingerprintBuilder.Result r = FingerprintBuilder.build(
                0L, 1L, 0L, "TRADE", 0L, 0L, 0L, 0L);

        assertNotNull(r.hash());
        assertEquals(64, r.hash().length(), "SHA-256 hex is 64 chars");
        assertEquals(1, r.version());
        assertEquals("SHA-256", r.algorithm());
    }

    @Test
    @DisplayName("Hash is lowercase hex")
    void hashIsLowercaseHex() {
        FingerprintBuilder.Result r = FingerprintBuilder.build(
                0L, 1L, 1L, "TRADE", 1L, 1L, 1L, 1L);

        assertEquals(r.hash(), r.hash().toLowerCase(), "Hash must be lowercase");
        assertTrue(r.hash().matches("^[0-9a-f]{64}$"), "Must be 64 lowercase hex chars");
    }
}
