package com.trading.ingestion.quarantine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QuarantineWriterTest {
    @Test
    void sanitizesCredentialBearingDetailsAndBoundsLength() {
        String detail = "ARROW_TOKEN=secret Authorization:Bearer-secret " + "x".repeat(800);
        String safe = QuarantineWriter.sanitizeDetail(detail);
        assertFalse(safe.contains("secret"));
        assertTrue(safe.contains("ARROW_TOKEN=[REDACTED]"));
        assertTrue(safe.length() <= 512);
    }

    // ING-SEC-RED-001 — inject every mandated secret class into a detail string
    // and assert none survive the Java boundary sanitizer.
    @Test
    void ingSecRed001SecretRedaction() {
        String detail = String.join(" | ",
                "ARROW_APP_SECRET=superSecretAppSecret123",
                "ARROW_PASSWORD=P@ssw0rd!secret",
                "ARROW_TOTP_KEY=JBSWY3DPEHPK3PXP",
                "ARROW_TOKEN=eyJhbGciOiJIUzI1NiJ9.secret",
                "access_token=ghp_secretToken456",
                "token=abcd1234secret",
                "appID=b3b40c832fcd",
                "Authorization=Bearer secretBearerToken",
                "https://socket.arrow.trade?appID=b3b40c832fcd&token=secretQueryToken");
        String safe = QuarantineWriter.sanitizeDetail(detail);
        // No raw secret values survive (the only acceptable occurrence is the
        // literal placeholder [REDACTED]).
        assertFalse(safe.contains("superSecretAppSecret123"));
        assertFalse(safe.contains("P@ssw0rd"));
        assertFalse(safe.contains("JBSWY3DPEHPK3PXP"));
        assertFalse(safe.contains("eyJhbGciOiJIUzI1NiJ9"));
        assertFalse(safe.contains("ghp_secretToken456"));
        assertFalse(safe.contains("abcd1234secret"));
        assertFalse(safe.contains("b3b40c832fcd"));
        assertFalse(safe.contains("secretBearerToken"));
        assertFalse(safe.contains("secretQueryToken"));
        assertTrue(safe.length() <= 512, "detail must be bounded to 512 chars");
    }
}
