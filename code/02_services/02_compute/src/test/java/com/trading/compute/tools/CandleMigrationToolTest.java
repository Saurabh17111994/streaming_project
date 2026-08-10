package com.trading.compute.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.signaljob.CandleTableColumns;
import com.trading.compute.tools.CandleMigrationTool.Audit;
import com.trading.compute.tools.CandleMigrationTool.KeyAgg;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.flink.table.data.StringData;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * CANDLE-KV-REPLAY-001 B8.2/B8.3: pure audit/merge logic of
 * {@link CandleMigrationTool}. Exercises the canonical filter, per-key
 * grouping, business-conflict detection (every column except
 * {@code output_ts}), duplicate counting, and the MAX(output_ts) merge target
 * with in-memory rows — no Fluss cluster required.
 */
@DisplayName("CandleMigrationTool audit/merge logic (B8.2/B8.3)")
class CandleMigrationToolTest {

    private static final String SCHEMA_V = "2";
    private static final String ALGO = "candle-15s-v1";
    private static final String CONFIG = "1.0.0";

    private static GenericRow candle(long token, long windowStart, long open, long outputTs) {
        return GenericRow.of(
                token, BinaryString.fromString("NSE"), BinaryString.fromString("TEST"),
                windowStart, windowStart + 15_000L,
                open, open + 10, open - 10, open + 5, 42L, 7,
                BinaryString.fromString(ALGO), BinaryString.fromString(CONFIG),
                outputTs, BinaryString.fromString(SCHEMA_V));
    }

    private static InternalRow nonCanonical(String schemaV, String algo, String config,
                                            long outputTs) {
        return GenericRow.of(
                1L, BinaryString.fromString("NSE"), BinaryString.fromString("TEST"),
                1000L, 1015L,
                100L, 110L, 90L, 105L, 42L, 7,
                BinaryString.fromString(algo), BinaryString.fromString(config),
                outputTs, BinaryString.fromString(schemaV));
    }

    @Test
    @DisplayName("replay re-emissions converge: same key, same business fields, "
            + "different output_ts — no conflict, one MAX(output_ts) target")
    void replayReEmissionConverges() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_250_010_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_250_010_000L, 15050, 200L)); // replay
        audit.add(candle(1660, 1_786_250_010_000L, 15050, 150L)); // replay

        assertEquals(3, audit.totalRows);
        assertEquals(3, audit.canonicalRows);
        assertEquals(0, audit.nonCanonicalRows);
        assertEquals(1, audit.distinctKeys());
        assertEquals(1, audit.duplicateKeys);
        assertEquals(0, audit.conflictingKeys);
        assertTrue(audit.conflictExamples.isEmpty());

        KeyAgg agg = audit.byKey.get(1660L).get(1_786_250_010_000L);
        assertNotNull(agg);
        assertEquals(3, agg.rows);
        assertEquals(200L, agg.maxOutputTs,
                "MAX(output_ts) wins the merge target");
        assertEquals(200L, agg.rowAtMaxOutputTs.getLong(CandleTableColumns.OUTPUT_TS));
    }

    @Test
    @DisplayName("distinct keys stay distinct rows; same key with different business "
            + "values is a conflict (abort gate)")
    void distinctKeysAndBusinessConflict() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_250_010_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_250_025_000L, 15100, 110L)); // new window
        audit.add(candle(5000, 1_786_250_010_000L, 9000, 120L));  // new instrument

        assertEquals(3, audit.distinctKeys());
        assertEquals(0, audit.conflictingKeys);
        assertEquals(0, audit.duplicateKeys);

        // Genuinely different candle for the same key — B8.2 aborts.
        Audit conflict = new Audit(SCHEMA_V, ALGO, CONFIG);
        conflict.add(candle(1660, 1_786_250_010_000L, 15050, 100L));
        conflict.add(candle(1660, 1_786_250_010_000L, 15555, 200L)); // open differs
        assertEquals(1, conflict.conflictingKeys);
        assertEquals(1, conflict.duplicateKeys);
        assertEquals(1, conflict.conflictExamples.size());
        assertTrue(conflict.conflictExamples.get(0).contains("open_paise"),
                "conflict names the first differing business field");
    }

    @Test
    @DisplayName("non-canonical rows are filtered and reported separately")
    void nonCanonicalRowsReported() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_250_010_000L, 15050, 100L)); // canonical
        audit.add(nonCanonical(SCHEMA_V, "candle-15s-v0", CONFIG, 200L)); // stale algorithm
        audit.add(nonCanonical(SCHEMA_V, ALGO, "0.9.0", 200L));           // stale config
        audit.add(nonCanonical("1", ALGO, CONFIG, 200L));                 // old schema version
        audit.add(nonCanonical(SCHEMA_V, "", CONFIG, 200L));                 // blank algorithm

        assertEquals(5, audit.totalRows);
        assertEquals(1, audit.canonicalRows);
        assertEquals(4, audit.nonCanonicalRows);
        assertEquals(1, audit.distinctKeys());
        assertEquals(0, audit.conflictingKeys);
    }

    @Test
    @DisplayName("non-output_ts columns all participate in conflict detection")
    void outputTsIsExcludedFromIdentity() {
        // tick_count differs — a business field, not output_ts → conflict.
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        GenericRow a = candle(1660, 1_786_250_010_000L, 15050, 100L);
        GenericRow b = candle(1660, 1_786_250_010_000L, 15050, 200L);
        b.setField(CandleTableColumns.TICK_COUNT, 9);
        audit.add(a);
        audit.add(b);

        assertEquals(1, audit.conflictingKeys);
        assertEquals("tick_count", audit.byKey.get(1660L)
                .get(1_786_250_010_000L).conflictField);
    }

    @Test
    @DisplayName("canonical policy exact-match: blank and padded values never match")
    void exactMatchSemantics() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(nonCanonical(SCHEMA_V, " candle-15s-v1", CONFIG, 100L)); // padded
        audit.add(nonCanonical(SCHEMA_V, "", CONFIG, 100L));                // blank
        assertEquals(0, audit.canonicalRows);
        assertEquals(2, audit.nonCanonicalRows);
        assertFalse(com.trading.compute.signaljob.CanonicalCandlePolicy.isCanonical(
                "", CONFIG, ALGO, CONFIG));
        assertNull(audit.byKey.get(1L));
    }

    @Test
    @DisplayName("accepted conflict keys merge by MAX(output_ts) and never abort")
    void acceptedConflictKeyMergesByMaxOutputTs() {
        // Recorded data-ops decision (CANDLE-MIGRATION-001, 2026-08-10):
        // accept the MAX(output_ts) row for the replay-incident keys even
        // though their business values differ.
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG, Set.of("1660:1786258020000"));
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L)); // open differs ±500

        assertEquals(1, audit.conflictingKeys);
        assertEquals(1, audit.acceptedKeysCount);
        assertEquals(0, audit.unacceptedConflictingKeys);
        assertEquals(0, audit.acceptedKeysNotFound());
        assertTrue(audit.conflictExamples.isEmpty(),
                "accepted conflicts are not actionable abort examples");

        KeyAgg agg = audit.byKey.get(1660L).get(1_786_258_020_000L);
        assertEquals(200L, agg.maxOutputTs);
        assertEquals(15550L, agg.rowAtMaxOutputTs.getLong(CandleTableColumns.OPEN_PAISE),
                "MAX(output_ts) row wins the merge target for accepted keys");
    }

    @Test
    @DisplayName("conflicts outside the accept list still abort (fail-closed)")
    void unacceptedConflictStillCounts() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG, Set.of("9999:1"));
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));

        assertEquals(1, audit.conflictingKeys);
        assertEquals(0, audit.acceptedKeysCount);
        assertEquals(1, audit.unacceptedConflictingKeys);
        assertEquals(1, audit.conflictExamples.size());
    }

    @Test
    @DisplayName("mixed accept coverage: accepted and unaccepted counted separately")
    void mixedAcceptCoverage() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG, Set.of("1660:1786258020000"));
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));  // accepted
        audit.add(candle(5000, 1_786_258_020_000L, 9000, 100L));
        audit.add(candle(5000, 1_786_258_020_000L, 9500, 200L));   // not accepted

        assertEquals(2, audit.conflictingKeys);
        assertEquals(1, audit.acceptedKeysCount);
        assertEquals(1, audit.unacceptedConflictingKeys);
        assertEquals(1, audit.conflictExamples.size());
        assertTrue(audit.conflictExamples.get(0).contains("token=5000"));
    }

    @Test
    @DisplayName("accept-list entries matching no canonical key are detected (typo/stale list)")
    void acceptedKeyNotFoundDetected() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG,
                Set.of("1660:1786258020000", "777:1")); // 777:1 never appears
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));

        assertEquals(1, audit.acceptedKeysNotFound());
    }

    @Test
    @DisplayName("accept-keys file parsing: comments, blanks, trimmed entries, dedupe")
    void acceptKeysFileParsing() throws Exception {
        Path p = Files.createTempFile("accept-keys", ".csv");
        try {
            Files.writeString(p, "# CANDLE-MIGRATION-001 decision (2026-08-10)\n"
                    + " 1660, 1786258020000 \n"
                    + "\n"
                    + "1660,1786258020000\n"   // duplicate — deduped by the Set
                    + "5000,1786258020000\n");
            Set<String> keys = CandleMigrationTool.loadAcceptKeys(p.toString());
            assertEquals(Set.of("1660:1786258020000", "5000:1786258020000"), keys);
        } finally {
            Files.deleteIfExists(p);
        }
    }

    @Test
    @DisplayName("accept-keys file: unset or malformed fails closed")
    void acceptKeysFileFailClosed() throws Exception {
        assertTrue(CandleMigrationTool.loadAcceptKeys(null).isEmpty(),
                "no accept file = pre-decision behavior (abort on any conflict)");

        Path p = Files.createTempFile("accept-keys", ".csv");
        try {
            Files.writeString(p, "1660,1786258020000\nnot-a-key\n");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CandleMigrationTool.loadAcceptKeys(p.toString()));
            assertTrue(e.getMessage().contains("line 2"),
                    "malformed line is named: " + e.getMessage());
        } finally {
            Files.deleteIfExists(p);
        }

        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationTool.loadAcceptKeys("/nonexistent/accept-keys.csv"));
    }
}
