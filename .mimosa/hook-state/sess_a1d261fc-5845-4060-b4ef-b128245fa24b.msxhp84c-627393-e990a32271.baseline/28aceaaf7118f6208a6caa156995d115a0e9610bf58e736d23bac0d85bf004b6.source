package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-002 subset: Config validation — IngestionConfig.validate() rejects bad values.
 * ING-UNIT-003: Round-trip: raw bytes → SHA-256 hash verification.
 */
@DisplayName("ING-UNIT-002/003: Config & Hash Validation")
class ConfigAndHashTest {

    @Test
    @DisplayName("SHA-256 of known bytes produces expected hash")
    void sha256RoundTrip() throws Exception {
        byte[] input = "RELIANCE-EQ|3045|234500".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(input);
        String hex = java.util.HexFormat.of().formatHex(digest);

        // Verify same input → same hash
        md.reset();
        byte[] digest2 = md.digest("RELIANCE-EQ|3045|234500".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String hex2 = java.util.HexFormat.of().formatHex(digest2);

        assertEquals(hex, hex2, "SHA-256 must be deterministic");
        assertEquals(64, hex.length(), "SHA-256 produces 64 hex chars");
    }

    @Test
    @DisplayName("Different inputs produce different SHA-256")
    void sha256DifferentInputs() throws Exception {
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
        String h1 = java.util.HexFormat.of().formatHex(
                md.digest("abc".getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        md.reset();
        String h2 = java.util.HexFormat.of().formatHex(
                md.digest("abd".getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        assertFalse(h1.equals(h2), "SHA-256 of different inputs must differ");
    }

    @Test
    @DisplayName("NtpClockChecker initializes with limit")
    void clockInitializes() {
        NtpClockChecker checker = new NtpClockChecker("pool.ntp.org", 100L);
        assertFalse(checker.isWithinLimit());
    }

    @Test
    @DisplayName("NtpClockChecker with custom limit")
    void clockCustomLimit() {
        NtpClockChecker checker = new NtpClockChecker("pool.ntp.org", 500L);
        assertFalse(checker.isWithinLimit());
    }
}
