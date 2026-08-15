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

    // ---- ING-UNIT-016: canonical-form golden pin ----

    @Test
    @DisplayName("ING-UNIT-016: the v1 canonical form is frozen — golden-hash snapshot across commits")
    void goldenHashFrozen() {
        // v1 canonical bytes: epoch(8 BE) | token(8 BE) | event_ms(8 BE) |
        // tickType(UTF-8) | ltp(8 BE) | qty(8 BE) | bid(8 BE) | ask(8 BE),
        // pipe-delimited, no trailing delimiter. Any silent change to field
        // order, encoding, null representation, or the version constant fails
        // this cross-commit pin. (Golden value computed 2026-08-15.)
        FingerprintBuilder.Result r = FingerprintBuilder.build(
                0L, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        assertEquals("aec03d3dc1c556134a8dcdee1d5d94796e9e24d42aed5a1018b57c4e93bc5e31",
                r.hash(), "canonical v1 hash must never change");
        assertEquals(1, r.version(), "version must stay 1 while the form is frozen");
        assertEquals("SHA-256", r.algorithm());
    }

    // ---- ING-UNIT-017: DEC-012 dedup semantics ----

    @Test
    @DisplayName("ING-UNIT-017: same tick, same epoch → identical; next epoch → different (DEC-012 best-effort identity)")
    void dedupSemanticsPinned() {
        // DEC-012: the fingerprint is best-effort identity within an epoch —
        // duplicates within the SAME epoch are expected (Compute owns logical
        // dedup), and a reconnect bumps the epoch which RESETS identity.
        long epoch = 7L;
        FingerprintBuilder.Result first = FingerprintBuilder.build(
                epoch, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);
        FingerprintBuilder.Result duplicate = FingerprintBuilder.build(
                epoch, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);
        FingerprintBuilder.Result nextEpoch = FingerprintBuilder.build(
                epoch + 1, 3045L, 1719000000000L, "TRADE",
                234500L, 50L, 234400L, 234600L);

        assertEquals(first.hash(), duplicate.hash(),
                "a duplicate within an epoch is expected — Compute dedups (DEC-012)");
        assertNotEquals(first.hash(), nextEpoch.hash(),
                "a reconnect bumps the epoch and must reset identity (DEC-012)");
    }
}
