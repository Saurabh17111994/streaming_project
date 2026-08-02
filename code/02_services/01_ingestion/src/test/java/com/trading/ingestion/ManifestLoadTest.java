package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.ingestion.model.Instrument;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ING-INT-001: Manifest load from weekly CSV, subscription completeness.
 *
 * <p>Loads the real Arrow broker instrument CSV from
 * {@code Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv} and
 * verifies all ~2400 instruments are parsed with required fields.
 *
 * <p>Set {@code INGESTION_INT_TEST_MANIFEST=true} to run.
 */
@DisplayName("ING-INT-001: Manifest Load")
class ManifestLoadTest {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestLoadTest.class);

    @Test
    @DisplayName("Load NSE CSV manifest — all instruments parsed, non-null fields")
    void loadProductionManifest() {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_MANIFEST", "false")),
                "Skipping — set INGESTION_INT_TEST_MANIFEST=true");

        InstrumentManifestLoader.ManifestResult result = InstrumentManifestLoader.loadFromPath(
                "/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/"
                        + "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv");

        assertTrue(result.approved(), "manifest should be approved");
        assertEquals(1, result.version());
        assertTrue(result.instrumentCount() > 2000,
                "expected >2000 instruments, got " + result.instrumentCount());

        List<Instrument> instruments = result.instruments();
        LOG.info("manifest: loaded {} instruments", instruments.size());

        for (Instrument inst : instruments) {
            assertTrue(inst.instrumentToken() > 0, "token must be positive");
            assertNotNull(inst.tradingSymbol(), "symbol must not be null");
            assertTrue(!inst.tradingSymbol().isBlank(), "symbol must not be blank");
            assertEquals("NSE", inst.exchange());
            assertTrue(inst.lotSize() > 0, "lot size must be positive");
        }

        // Known symbol should be present
        boolean hasReliance = instruments.stream()
                .anyMatch(i -> "RELIANCE-EQ".equals(i.tradingSymbol()));
        assertTrue(hasReliance, "RELIANCE-EQ should be in manifest");

        // Fingerprint is deterministic SHA-256
        String fp = InstrumentManifestLoader.computeFingerprint(instruments);
        assertEquals(64, fp.length(), "SHA-256 hex is 64 chars");
        LOG.info("manifest: fingerprint={}", fp.substring(0, 12));
    }

    @Test
    @DisplayName("Quoted header CSV is parsed with correct columns")
    void parsesQuotedHeader() {
        List<String> cols = InstrumentManifestLoader.parseCsvRecord(
                "\"Exchange\",\"Segment\",\"ExchSeg\",\"Token\",\"TradingSymbol\"");
        assertEquals(5, cols.size());
        assertEquals("Exchange", cols.get(0));
        assertEquals("Token", cols.get(3));
        assertEquals("TradingSymbol", cols.get(4));
    }

    @Test
    @DisplayName("Synthetic manifest tokens match MockArrowServer formula (R-027)")
    void syntheticSetMatchesMockArrowServer() {
        // R-027 regression: the synthetic set used 100_000 + i*100 + (i%10),
        // which shares only 5 tokens with MockArrowServer's 100_000 + i*100 —
        // so fake-broker ticks were mostly quarantined as MISSING_INSTRUMENT
        // and the subscription-completeness check never passed.
        List<Instrument> set = InstrumentManifestLoader.syntheticSet();
        assertEquals(50, set.size(), "must be 50 synthetic instruments");

        for (int i = 0; i < 50; i++) {
            long expected = 100_000L + i * 100L;
            assertTrue(set.stream().anyMatch(inst -> inst.instrumentToken() == expected),
                    "token " + expected + " (MockArrowServer formula) missing from set");
        }
        assertEquals(50, set.stream().map(Instrument::instrumentToken).distinct().count(),
                "all 50 tokens must be unique");

        // The exact 5-token overlap that the old formula produced must be gone:
        // 100_000 + i*100 + (i%10) only coincided at i%10 == 0 (5 values).
        assertEquals(50, set.stream()
                .filter(inst -> inst.instrumentToken() >= 100_000L && inst.instrumentToken() <= 104_900L)
                .count(), "all tokens must be within the MockArrowServer range");
    }

    @Test
    @DisplayName("Quoted field containing a comma is preserved as one field")
    void quotedCommaPreserved() {
        List<String> cols = InstrumentManifestLoader.parseCsvRecord(
                "1,\"Final Dividend - Rs. - 0.6500;23 Jul 2026\",2");
        assertEquals(3, cols.size());
        assertEquals("Final Dividend - Rs. - 0.6500;23 Jul 2026", cols.get(1));
    }

    @Test
    @DisplayName("Escaped quotes are unescaped correctly")
    void escapedQuoteUnescaped() {
        List<String> cols = InstrumentManifestLoader.parseCsvRecord(
                "\"a\"\"b\"\"c\"");
        assertEquals(1, cols.size());
        assertEquals("a\"b\"c", cols.get(0));
    }

    @Test
    @DisplayName("Unterminated quoted field is rejected")
    void unterminatedQuoteRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> InstrumentManifestLoader.parseCsvRecord("\"unterminated"));
    }

    @Test
    @DisplayName("Real quoted 1024-manifest file loads 1024 instruments")
    void loadsRealQuotedManifest() {
        String path = "/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/"
                + "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv";
        if (!java.nio.file.Files.exists(java.nio.file.Path.of(path))) {
            assumeTrue(false, "Real quoted manifest not present on this machine");
        }
        InstrumentManifestLoader.ManifestResult result =
                InstrumentManifestLoader.loadFromPath(path);
        assertTrue(result.approved(), "manifest should be approved");
        assertEquals(1024, result.instrumentCount(),
                "expected 1024 instruments, got " + result.instrumentCount());
        assertEquals(1024, result.instruments().size());
    }
}
