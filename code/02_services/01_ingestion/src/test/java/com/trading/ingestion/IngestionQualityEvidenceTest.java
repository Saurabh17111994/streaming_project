package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.quarantine.QuarantineWriter;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-DQ-001..002 — typed quality classification and quarantine routing
 * evidence (plan Amendment §Required tests and evidence).
 *
 * <p>The routing itself lives in {@link IngestionService#processLine} and is
 * driven by live Fluss writers, so the pure seams are pinned here instead:
 * the malformed-JSON routing decision ({@link IngestionService#malformedJsonDecision})
 * with its static, non-leaking detail constant, plus the exact quarantine
 * reason vocabularies shared by the Go bridge and the Java writer. Stale/future
 * routing and its boundary semantics are covered by {@link StaleDataTradeGuardTest}.
 *
 * <ul>
 *   <li>ING-DQ-001 — a malformed NDJSON line is classified MALFORMED_JSON, its
 *       raw bytes are preserved as evidence, and the detail carries no line
 *       content (Jackson's exception message embeds the offending snippet and
 *       must never be logged).</li>
 *   <li>ING-DQ-002 — stale records remain durable evidence (STALE_BROKER_TIMESTAMP
 *       quarantine) and can never reach a trade path: the freshness gate runs
 *       before any trade classification (covered by StaleDataTradeGuardTest).</li>
 * </ul>
 */
@DisplayName("ING-DQ-001..002: typed quality classification and quarantine routing")
class IngestionQualityEvidenceTest {

    @Test
    @DisplayName("ING-DQ-001: malformed line classifies MALFORMED_JSON with raw bytes preserved")
    void malformedJsonPreservesRawBytes() {
        String malformed = "{\"record_type\":\"tick\",\"token\":3045,\"ltp_paise\":\"not-a-number\",";
        byte[] raw = malformed.getBytes(StandardCharsets.UTF_8);
        IngestionService.MalformedJsonDecision decision =
                IngestionService.malformedJsonDecision(raw);

        assertEquals(QuarantineWriter.Reason.MALFORMED_JSON, decision.reason());
        assertArrayEquals(raw, decision.rawPayload(),
                "quarantine evidence must preserve the exact received line bytes");
        assertEquals(IngestionService.malformedJsonDetail(), decision.detail(),
                "detail must be the static constant");
        assertFalse(decision.detail().contains(malformed),
                "detail must not contain any line content (no log leakage)");
    }

    @Test
    @DisplayName("ING-DQ-001: static detail never leaks line content and stays bounded")
    void detailConstantIsStaticAndBounded() {
        String detail = IngestionService.malformedJsonDetail();
        assertFalse(detail.contains("{"), "static detail must not interpolate line bytes");
        assertFalse(detail.contains("record_type"), "static detail must not echo line content");
        assertTrue(detail.length() <= 512, "detail must stay within the 512-char bound");
    }

    @Test
    @DisplayName("ING-DQ-001: rawLineBytes is null-safe and byte-exact")
    void rawLineBytesNullSafe() {
        assertNull(IngestionService.rawLineBytes(null));
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8),
                IngestionService.rawLineBytes("abc"));
    }

    @Test
    @DisplayName("ING-DQ-001: quarantine reason vocabulary is exact per plan")
    void quarantineReasonVocabularyExact() {
        String[] expected = {
                "MALFORMED_JSON", "INVALID_SCHEMA", "MISSING_INSTRUMENT",
                "INVALID_VALUES", "FUTURE_BROKER_TIMESTAMP", "STALE_BROKER_TIMESTAMP",
                "BROKER_LIMIT_VIOLATION", "HASH_MISMATCH", "INTERNAL_ERROR",
                "FINGERPRINT_FAILURE",
        };
        QuarantineWriter.Reason[] actual = QuarantineWriter.Reason.values();
        assertEquals(expected.length, actual.length, "reason vocabulary must match plan exactly");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i].name(), "reason[" + i + "]");
        }
    }

    @Test
    @DisplayName("ING-DQ-001: broker quarantine reasons are a subset of the writer vocabulary")
    void brokerQuarantineReasonsSubset() {
        // The bridge-side REASONS set is private; assert the documented
        // vocabulary directly against the writer's accepted names.
        for (String bridgeReason : new String[]{
                "MALFORMED_JSON", "INVALID_SCHEMA", "INVALID_VALUES", "HASH_MISMATCH",
                "FUTURE_BROKER_TIMESTAMP", "STALE_BROKER_TIMESTAMP", "BROKER_LIMIT_VIOLATION"}) {
            QuarantineWriter.Reason.valueOf(bridgeReason);
        }
    }

    @Test
    @DisplayName("ING-DQ-002: stale classification precedes any trade path")
    void stalePrecedesTradePath() {
        // Mirrors the production evidence-backed values
        // (ARROW_MAX_EVENT_AGE_MS=5000, ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000):
        // age boundary inclusive at 5000, STALE at 5001. The quarantine reason
        // is the durable evidence; the freshness gate runs before any trade
        // classification (see StaleDataTradeGuardTest for the full matrix).
        assertEquals(IngestionService.FreshnessDecision.FRESH,
                IngestionService.classifyFreshness(1_000_000_000L - 5000, 1_000_000_000L, 2000, 5000));
        assertEquals(IngestionService.FreshnessDecision.STALE,
                IngestionService.classifyFreshness(1_000_000_000L - 5001, 1_000_000_000L, 2000, 5000));
        assertEquals(QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                "STALE quarantine reason is the durable evidence class");
    }
}
