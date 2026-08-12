package com.trading.compute.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.hash.Hashing;
import com.trading.compute.signaljob.CandleTableColumns;
import com.trading.compute.tools.CandleMigrationTool.AcceptEntry;
import com.trading.compute.tools.CandleMigrationTool.Audit;
import com.trading.compute.tools.CandleMigrationTool.KeyAgg;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.StringData;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * CANDLE-KV-REPLAY-001 B8.2/B8.3 + tracker 14 P3: pure audit/approval logic of
 * {@link CandleMigrationTool}. Exercises the canonical filter, per-key
 * grouping, business-conflict detection (every column except
 * {@code output_ts}), duplicate counting, SHA-256 row hashing, hash-driven
 * approved-row selection (never MAX(output_ts) merely because it is latest),
 * stale-approval rejection, the max-keys memory guard, and the Fluss bucket
 * mapping — with in-memory rows, no Fluss cluster required.
 */
@DisplayName("CandleMigrationTool audit/merge logic (B8.2/B8.3, tracker 14 P3)")
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

    /** One-key approval map pinning the given row (hash-driven — tracker 14 P3.1). */
    private static Map<String, AcceptEntry> approve(long token, long windowStart,
                                                    InternalRow approvedRow) {
        return Map.of(token + ":" + windowStart,
                new AcceptEntry(token, windowStart, CandleMigrationTool.rowHash(approvedRow),
                        "APPROVE"));
    }

    @Test
    @DisplayName("replay re-emissions converge: same key, same business fields, "
            + "different output_ts — no conflict, one MAX(output_ts) target")
    void replayReEmissionConverges() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 200L)); // same business, later emit

        assertEquals(2, audit.totalRows);
        assertEquals(2, audit.canonicalRows);
        assertEquals(0, audit.nonCanonicalRows);
        assertEquals(1, audit.distinctKeys());
        assertEquals(1, audit.duplicateKeys);
        assertEquals(0, audit.conflictingKeys);
        assertEquals(0, audit.unacceptedConflictingKeys);

        KeyAgg agg = audit.byKey.get(1660L).get(1_786_258_020_000L);
        assertEquals(200L, agg.maxOutputTs);
        // Conflict-free: any business-identical row is equivalent, MAX(output_ts) is fine.
        assertEquals(15050L,
                audit.approvedRow(agg).getLong(CandleTableColumns.OPEN_PAISE));
    }

    @Test
    @DisplayName("distinct keys stay distinct rows; same key with different business "
            + "values is a conflict (abort gate)")
    void distinctKeysAndBusinessConflict() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L)); // open differs ±500
        audit.add(candle(5000, 1_786_258_020_000L, 9000, 100L));

        assertEquals(2, audit.distinctKeys());
        assertEquals(1, audit.conflictingKeys);
        assertEquals(1, audit.unacceptedConflictingKeys);
        assertEquals(1, audit.conflictExamples.size());
        assertNull(audit.approvedRow(audit.byKey.get(1660L).get(1_786_258_020_000L)),
                "unapproved conflict has no approved row");
    }

    @Test
    @DisplayName("non-canonical rows are filtered and reported separately")
    void nonCanonicalRowsReported() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(nonCanonical("2", "candle-15s-v2", "1.0.0", 100L));
        audit.add(nonCanonical("3", "candle-15s-v1", "1.0.0", 100L));
        audit.add(nonCanonical("2", "candle-15s-v1", "2.0.0", 100L));

        assertEquals(1, audit.canonicalRows);
        assertEquals(3, audit.nonCanonicalRows);
        assertEquals(1, audit.distinctKeys());
    }

    @Test
    @DisplayName("non-output_ts columns all participate in conflict detection")
    void outputTsIsExcludedFromIdentity() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        // Same business fields, different window_end — a REAL business conflict
        GenericRow differentWindowEnd = GenericRow.of(
                1660L, BinaryString.fromString("NSE"), BinaryString.fromString("TEST"),
                1_786_258_020_000L, 1_786_258_050_000L, // window_end differs (15s vs 30s)
                15050L, 15060L, 15040L, 15055L, 42L, 7,
                BinaryString.fromString(ALGO), BinaryString.fromString(CONFIG),
                100L, BinaryString.fromString(SCHEMA_V));
        audit.add(differentWindowEnd);

        assertEquals(1, audit.conflictingKeys);
        assertTrue(audit.conflictExamples.get(0).contains("window_end"));
    }

    @Test
    @DisplayName("canonical policy exact-match: blank and padded values never match")
    void exactMatchSemantics() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(nonCanonical("2", "candle-15s-v1 ", "1.0.0", 100L)); // padded algorithm
        audit.add(nonCanonical("2", "", "1.0.0", 100L));               // blank algorithm

        assertEquals(1, audit.canonicalRows);
        assertEquals(2, audit.nonCanonicalRows);
    }

    @Test
    @DisplayName("P3.1: approved conflict keys merge by the approved-row HASH, never "
            + "MAX(output_ts) merely because it is latest")
    void acceptedConflictKeyMergesByApprovedHash() {
        InternalRow earlier = candle(1660, 1_786_258_020_000L, 15050, 100L);
        InternalRow later = candle(1660, 1_786_258_020_000L, 15550, 200L); // max output_ts

        // Approval pins the EARLIER (lower output_ts) row — it must win despite
        // not being latest.
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG,
                approve(1660, 1_786_258_020_000L, earlier));
        audit.add(earlier);
        audit.add(later);
        audit.resolveAll();

        assertEquals(1, audit.conflictingKeys);
        assertEquals(1, audit.acceptedKeysCount);
        assertEquals(0, audit.unacceptedConflictingKeys);
        assertEquals(0, audit.approvalStaleKeys);
        KeyAgg agg = audit.byKey.get(1660L).get(1_786_258_020_000L);
        assertEquals(200L, agg.maxOutputTs, "max output_ts is 200 — but approval decides");
        assertEquals(15050L, audit.approvedRow(agg).getLong(CandleTableColumns.OPEN_PAISE),
                "approved row is the hash-matched candidate, not the latest row");

        // Approval pinning the later row selects that one instead.
        Audit audit2 = new Audit(SCHEMA_V, ALGO, CONFIG,
                approve(1660, 1_786_258_020_000L, later));
        audit2.add(earlier);
        audit2.add(later);
        audit2.resolveAll();
        KeyAgg agg2 = audit2.byKey.get(1660L).get(1_786_258_020_000L);
        assertEquals(15550L, audit2.approvedRow(agg2).getLong(CandleTableColumns.OPEN_PAISE));
    }

    @Test
    @DisplayName("P3.1: wrong/stale approval (hash matches no candidate) fails closed")
    void staleApprovalFailsClosed() {
        InternalRow unrelated = candle(1660, 1_786_258_020_000L, 9999, 999L); // hash matches nothing
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG,
                approve(1660, 1_786_258_020_000L, unrelated));
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));
        audit.resolveAll();

        assertEquals(1, audit.approvalStaleKeys);
        assertEquals(1, audit.blockedKeys);
        KeyAgg agg = audit.byKey.get(1660L).get(1_786_258_020_000L);
        assertNull(audit.approvedRow(agg),
                "stale approval must exclude the key from load (exit 1 in main)");
        assertTrue(agg.approvalStale);
    }

    @Test
    @DisplayName("P3.1: missing approval for a conflicting key is the abort gate (exit 2)")
    void unacceptedConflictStillCounts() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG,
                approve(9999, 1, candle(9999, 1, 1, 1))); // approval for a different key
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));

        assertEquals(1, audit.conflictingKeys);
        assertEquals(0, audit.acceptedKeysCount);
        assertEquals(1, audit.unacceptedConflictingKeys);
    }

    @Test
    @DisplayName("mixed approval coverage: accepted and unaccepted counted separately")
    void mixedAcceptCoverage() {
        InternalRow acceptedRow = candle(1660, 1_786_258_020_000L, 15050, 100L);
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG,
                approve(1660, 1_786_258_020_000L, acceptedRow));
        audit.add(acceptedRow);
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));  // approved conflict
        audit.add(candle(5000, 1_786_258_020_000L, 9000, 100L));
        audit.add(candle(5000, 1_786_258_020_000L, 9500, 200L));   // not approved
        audit.resolveAll();

        assertEquals(2, audit.conflictingKeys);
        assertEquals(1, audit.acceptedKeysCount);
        assertEquals(1, audit.unacceptedConflictingKeys);
        assertEquals(1, audit.conflictExamples.size());
        assertTrue(audit.conflictExamples.get(0).contains("token=5000"));
    }

    @Test
    @DisplayName("approval entries matching no canonical key are detected (typo/stale list)")
    void acceptedKeyNotFoundDetected() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG, Map.of(
                "1660:1786258020000",
                new AcceptEntry(1660, 1_786_258_020_000L,
                        CandleMigrationTool.rowHash(candle(1660, 1_786_258_020_000L, 15050, 100L)),
                        "APPROVE"),
                "777:1", new AcceptEntry(777, 1, "a".repeat(64), "APPROVE"))); // never appears
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));

        assertEquals(1, audit.acceptedKeysNotFound(audit.seenKeys()));
    }

    @Test
    @DisplayName("P3.1: SHA-256 row hash — deterministic, output_ts excluded, business change alters it")
    void rowHashSemantics() {
        InternalRow a = candle(1660, 1_786_258_020_000L, 15050, 100L);
        InternalRow b = candle(1660, 1_786_258_020_000L, 15050, 200L); // only output_ts differs
        InternalRow c = candle(1660, 1_786_258_020_000L, 15550, 100L); // open differs

        assertEquals(CandleMigrationTool.rowHash(a), CandleMigrationTool.rowHash(a),
                "deterministic");
        assertEquals(CandleMigrationTool.rowHash(a), CandleMigrationTool.rowHash(b),
                "output_ts is emit metadata, not row identity (CanonicalCandlePolicy)");
        assertFalse(CandleMigrationTool.rowHash(a).equals(CandleMigrationTool.rowHash(c)),
                "any differing business field must change the hash");
        assertTrue(CandleMigrationTool.rowHash(a).matches("[0-9a-f]{64}"),
                "64-hex-char SHA-256");
    }

    @Test
    @DisplayName("P3.3: distinct-key limit is enforced BEFORE allocation (no silent OOM)")
    void maxKeysGuard() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG, Map.of(), 2L);
        audit.add(candle(1, 1000, 15050, 100L));
        audit.add(candle(2, 2000, 15050, 100L));
        assertThrows(IllegalStateException.class,
                () -> audit.add(candle(3, 3000, 15050, 100L)),
                "third distinct key exceeds the limit and must abort");
    }

    @Test
    @DisplayName("P3.3: conflict records are emitted separately with hashes and versions")
    void conflictRecordsEmitted() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));
        audit.resolveAll();

        assertEquals(1, audit.conflictRecords().size());
        String record = audit.conflictRecords().get(0);
        assertTrue(record.contains("token=1660"), record);
        assertTrue(record.contains("candidates=["), record);
        assertTrue(record.contains("hash="), record);
        assertTrue(record.contains("algorithm=" + ALGO), record);
        assertTrue(record.contains("approved=MISSING"), record);
    }

    @Test
    @DisplayName("P3.1: approval resolution is idempotent — re-running the audit converges")
    void approvalResolutionIdempotent() {
        InternalRow earlier = candle(1660, 1_786_258_020_000L, 15050, 100L);
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG,
                approve(1660, 1_786_258_020_000L, earlier));
        audit.add(earlier);
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));
        audit.resolveAll();
        InternalRow first = audit.approvedRow(audit.byKey.get(1660L).get(1_786_258_020_000L));
        audit.resolveAll(); // rerun — idempotent, no state change
        InternalRow second = audit.approvedRow(audit.byKey.get(1660L).get(1_786_258_020_000L));
        assertEquals(first, second);
        assertEquals(15050L, second.getLong(CandleTableColumns.OPEN_PAISE));
    }

    // ── approval-file format (P3.1) ────────────────────────────────────────

    @Test
    @DisplayName("approval file parsing: comments, blanks, trimmed entries, dedupe")
    void acceptKeysFileParsing() throws Exception {
        String hash = CandleMigrationTool.rowHash(candle(1660, 1_786_258_020_000L, 15050, 100L));
        Path p = Files.createTempFile("accept-keys", ".csv");
        try {
            Files.writeString(p, "# MIGRATION-CONFLICT-002 approval (tracker 14 P3.1)\n"
                    + " 1660, 1786258020000, " + hash + ", APPROVE \n"
                    + "\n"
                    + "1660,1786258020000," + hash + ",approve\n"   // duplicate — deduped
                    + "5000,1786258020000," + hash + ",APPROVE\n");
            Map<String, AcceptEntry> entries = CandleMigrationTool.loadAcceptKeys(p.toString());
            assertEquals(2, entries.size());
            assertEquals("APPROVE", entries.get("1660:1786258020000").decision());
            assertEquals(hash, entries.get("1660:1786258020000").rowHash());
            assertEquals("APPROVE", entries.get("5000:1786258020000").decision());
        } finally {
            Files.deleteIfExists(p);
        }
    }

    @Test
    @DisplayName("approval file: unset is empty; legacy 2-field lines fail closed")
    void acceptKeysFileFailClosed() throws Exception {
        assertTrue(CandleMigrationTool.loadAcceptKeys(null).isEmpty(),
                "no approval file = pre-decision behavior (abort on any conflict)");

        Path p = Files.createTempFile("accept-keys", ".csv");
        try {
            // Legacy 2-field accept line — rejected with a pointer to the new format.
            Files.writeString(p, "1660,1786258020000\n");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CandleMigrationTool.loadAcceptKeys(p.toString()));
            assertTrue(e.getMessage().contains("legacy 2-field"),
                    "legacy format must be named: " + e.getMessage());
        } finally {
            Files.deleteIfExists(p);
        }

        Path q = Files.createTempFile("accept-keys", ".csv");
        try {
            // 4 fields but a malformed hash and a non-APPROVE decision.
            Files.writeString(q, "1660,1786258020000,not-a-hash,APPROVE\n"
                    + "5000,1786258020000," + "a".repeat(64) + ",IGNORE\n");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CandleMigrationTool.loadAcceptKeys(q.toString()));
            assertTrue(e.getMessage().contains("line 1"),
                    "first malformed line is named: " + e.getMessage());
        } finally {
            Files.deleteIfExists(q);
        }

        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationTool.loadAcceptKeys("/nonexistent/accept-keys.csv"));
    }

    // ── bucket mapping (P3.3 coverage) ─────────────────────────────────────

    @Test
    @DisplayName("P3.1: approval provenance (approver/reason/decidedAt) is parsed and recorded")
    void approvalProvenanceParsedAndRecorded() {
        String hash = CandleMigrationTool.rowHash(candle(1660, 1_786_258_020_000L, 15050, 100L));
        AcceptEntry withProvenance = AcceptEntry.parse(
                "1660,1786258020000," + hash + ",APPROVE,ops-jdoe,replay-incident-2026-08-10,2026-08-10T18:30:00Z",
                1, "test.csv");
        assertEquals("ops-jdoe", withProvenance.approver());
        assertEquals("replay-incident-2026-08-10", withProvenance.reason());
        assertEquals("2026-08-10T18:30:00Z", withProvenance.decidedAt());

        // 4-field lines remain valid (no provenance recorded)
        AcceptEntry bare = AcceptEntry.parse(
                "1660,1786258020000," + hash + ",APPROVE", 2, "test.csv");
        assertNull(bare.approver());
        assertNull(bare.reason());
        assertNull(bare.decidedAt());
        assertEquals(hash, bare.rowHash());

        // blank provenance fields are treated as absent
        AcceptEntry blankProvenance = AcceptEntry.parse(
                "1660,1786258020000," + hash + ",APPROVE,  , ,  ", 3, "test.csv");
        assertNull(blankProvenance.approver());
        assertNull(blankProvenance.decidedAt());

        // too many fields fail closed
        assertThrows(IllegalArgumentException.class, () -> AcceptEntry.parse(
                "1660,1786258020000," + hash + ",APPROVE,ops-jdoe,r,2026-08-10,extra", 4,
                "test.csv"));
    }

    @Test
    @DisplayName("P3.1: conflict record carries each candidate's full business values")
    void conflictRecordIncludesFullBusinessValues() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        audit.add(candle(1660, 1_786_258_020_000L, 15050, 100L));
        audit.add(candle(1660, 1_786_258_020_000L, 15550, 200L));
        audit.resolveAll();
        assertEquals(1, audit.conflictingKeys);
        List<String> records = audit.conflictRecords();
        assertEquals(1, records.size());
        String record = records.get(0);
        assertTrue(record.contains("approved=MISSING"), record);
        assertTrue(record.contains("values=instrument_token=1660"), record);
        assertTrue(record.contains("exchange=NSE"), record);
        assertTrue(record.contains("open_paise=15050"), record);
        assertTrue(record.contains("open_paise=15550"), record);
        assertTrue(record.contains("schema_version=2"), record);
        assertFalse(record.contains("output_ts="), "output_ts is emit metadata, not row identity");
    }

    @Test
    @DisplayName("P3.1: approval record lists chosen hash, rejected hashes, and provenance")
    void approvalRecordListsChosenRejectedAndProvenance() {
        InternalRow chosen = candle(1660, 1_786_258_020_000L, 15050, 100L); // NOT max output_ts
        InternalRow rejected = candle(1660, 1_786_258_020_000L, 15550, 200L);
        String chosenHash = CandleMigrationTool.rowHash(chosen);
        String rejectedHash = CandleMigrationTool.rowHash(rejected);
        assertFalse(chosenHash.equals(rejectedHash), "fixture must be a real conflict");

        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG, Map.of("1660:1786258020000",
                new AcceptEntry(1660, 1_786_258_020_000L, chosenHash, "APPROVE",
                        "ops-jdoe", "replay-incident-2026-08-10", "2026-08-10T18:30:00Z")));
        audit.add(rejected);
        audit.add(chosen);
        audit.resolveAll();
        assertEquals(0, audit.approvalStaleKeys);
        assertEquals(chosen, audit.approvedRow(
                audit.byKey.get(1660L).get(1_786_258_020_000L)));

        List<String> records = audit.approvalRecords();
        assertEquals(1, records.size());
        String record = records.get(0);
        assertTrue(record.contains("token=1660"), record);
        assertTrue(record.contains("approvedHash=" + chosenHash), record);
        assertTrue(record.contains("rejectedHashes=" + rejectedHash), record);
        assertTrue(record.contains("approver=ops-jdoe"), record);
        assertTrue(record.contains("reason=replay-incident-2026-08-10"), record);
        assertTrue(record.contains("decidedAt=2026-08-10T18:30:00Z"), record);
    }

    @Test
    @DisplayName("P3.4: a canonical row with a null key column fails closed (no fabricated key)")
    void nullKeyFailsClosed() {
        Audit audit = new Audit(SCHEMA_V, ALGO, CONFIG);
        GenericRow nullToken = GenericRow.of(
                null, BinaryString.fromString("NSE"), BinaryString.fromString("TEST"),
                1_786_258_020_000L, 1_786_258_035_000L,
                15050L, 15060L, 15040L, 15055L, 42L, 7,
                BinaryString.fromString(ALGO), BinaryString.fromString(CONFIG),
                100L, BinaryString.fromString(SCHEMA_V));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> audit.add(nullToken));
        assertTrue(e.getMessage().contains("null key"), e.getMessage());
    }

    @Test
    @DisplayName("murmur3_32_fixed matches Guava over little-endian 8-byte tokens")
    void murmur3CrossCheckedAgainstGuava() {
        long[] tokens = {1L, 1660L, 5000L, 123_456_789L, 9_999_999_999L, -1L, 0L,
                Long.MAX_VALUE, 10_201L, 40_483L};
        for (long token : tokens) {
            byte[] le = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(token).array();
            int expected = Hashing.murmur3_32_fixed().hashBytes(le).asInt();
            assertEquals(expected, CandleMigrationTool.murmur3_32FixedLittleEndian64(token),
                    "token=" + token);
        }
    }

    @Test
    @DisplayName("every token maps into exactly one bucket; same token always the same bucket")
    void bucketMappingIsStableAndTotal() {
        int numBuckets = 16;
        boolean[] covered = new boolean[numBuckets];
        for (long token = 1; token <= 2048; token++) {
            int b = CandleMigrationTool.bucketFor(token, numBuckets);
            assertTrue(b >= 0 && b < numBuckets, "bucket out of range: " + b);
            covered[b] = true;
            assertEquals(b, CandleMigrationTool.bucketFor(token, numBuckets),
                    "same token must always map to the same bucket (write-side colocation)");
        }
        for (int b = 0; b < numBuckets; b++) {
            assertTrue(covered[b], "bucket " + b + " must be reachable (none skipped)");
        }
    }
}
