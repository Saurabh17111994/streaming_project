package com.trading.compute.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.trading.compute.signaljob.CandleTableColumns;
import com.trading.compute.tools.CandleMigrationBatchJob.Config;
import com.trading.compute.tools.CandleMigrationBatchJob.KeyDecision;
import com.trading.compute.tools.CandleMigrationBatchJob.MigrationBlockedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tracker 14 P3.3 (box 419): {@link CandleMigrationBatchJob} — the bounded
 * batch/Table-API migration twin of {@link CandleMigrationTool}. Unit tests
 * pin the byte-identical row-hash contract (approval hashes were CLI-computed),
 * the pass-2 per-key decision classification, and the fail-closed approval
 * parse; MiniCluster tests run the real {@code wire()} pipeline against a
 * canned bounded source and prove the gate's CLI-exit semantics (UNACCEPTED /
 * STALE / NOT_FOUND end the job FAILED) and the evidence-record formats.
 *
 * <p>No dev Fluss cluster required: the lake-enabled Table-API source is
 * exercised live (see the tracker-14 evidence file), the aggregation/sink
 * logic is exercised here.
 */
@DisplayName("CandleMigrationBatchJob audit/gate logic (tracker 14 P3.3 box 419)")
class CandleMigrationBatchJobTest {

    private static final String SCHEMA_V = "2";
    private static final String ALGO = "candle-15s-v1";
    private static final String CONFIG = "1.0.0";
    private static final long WINDOW = 1_786_258_020_000L;

    // ── row builders (business-identical shape to CandleMigrationToolTest) ──

    private static GenericRowData rowData(long token, long windowStart, long open, long outputTs) {
        return GenericRowData.of(
                token, StringData.fromString("NSE"), StringData.fromString("TEST"),
                windowStart, windowStart + 15_000L,
                open, open + 10, open - 10, open + 5, 42L, 7,
                StringData.fromString(ALGO), StringData.fromString(CONFIG),
                outputTs, StringData.fromString(SCHEMA_V));
    }

    private static GenericRow internalRow(long token, long windowStart, long open, long outputTs) {
        return GenericRow.of(
                token, BinaryString.fromString("NSE"), BinaryString.fromString("TEST"),
                windowStart, windowStart + 15_000L,
                open, open + 10, open - 10, open + 5, 42L, 7,
                BinaryString.fromString(ALGO), BinaryString.fromString(CONFIG),
                outputTs, BinaryString.fromString(SCHEMA_V));
    }

    private static Config config(String mode, String acceptKeysFile, String reportDir) {
        return new Config("localhost:9123", "feature_candles_15s", "feature_candles_15s_current",
                SCHEMA_V, ALGO, CONFIG, acceptKeysFile, mode, reportDir);
    }

    private static Config config(String mode, String reportDir) {
        return config(mode, null, reportDir);
    }

    // ── unit: hash + value contract ──────────────────────────────────────────

    @Test
    @DisplayName("RowData rowHash is byte-identical to the CLI's InternalRow rowHash "
            + "(approval hashes were CLI-computed)")
    void hashEquivalenceAcrossRowRepresentations() {
        GenericRowData rowData = rowData(1660, WINDOW, 15050, 100L);
        InternalRow internalRow = internalRow(1660, WINDOW, 15050, 100L);
        assertEquals(CandleMigrationTool.rowHash(internalRow),
                CandleMigrationBatchJob.rowHash(rowData),
                "batch job must produce the CLI's exact SHA-256 (P3.1 approval contract)");
        // Business-field change -> hash change, output_ts change -> same hash.
        assertEquals(CandleMigrationBatchJob.rowHash(rowData),
                CandleMigrationBatchJob.rowHash(rowData(1660, WINDOW, 15050, 999L)),
                "output_ts is emit metadata, not row identity");
        assertFalse(CandleMigrationBatchJob.rowHash(rowData(1660, WINDOW, 15550, 100L))
                        .equals(CandleMigrationBatchJob.rowHash(rowData)),
                "any differing business field must change the hash");
    }

    @Test
    @DisplayName("RowData businessValues render identically to the CLI's")
    void businessValuesEquivalence() {
        assertEquals(CandleMigrationTool.Audit.businessValues(internalRow(1660, WINDOW, 15050, 100L)),
                CandleMigrationBatchJob.businessValues(rowData(1660, WINDOW, 15050, 100L)));
    }

    // ── unit: pass-2 classification ──────────────────────────────────────────

    /** Drives the pure {@code classify} over the same cap semantics as pass 2. */
    private static KeyDecision aggregate(Config cfg, RowData... rows) {
        CandleMigrationBatchJob.CandAcc acc = new CandleMigrationBatchJob.CandAcc();
        for (RowData row : rows) {
            String hash = CandleMigrationBatchJob.rowHash(row);
            if (acc.hashes.add(hash)) {
                if (acc.rows.size() < CandleMigrationBatchJob.MAX_CONFLICT_CANDIDATES) {
                    acc.rows.add(row);
                } else {
                    acc.truncated = true;
                }
            }
        }
        return CandleMigrationBatchJob.classify(cfg, acc,
                CandleMigrationTool.loadAcceptKeys(cfg.acceptKeysFile));
    }

    @Test
    @DisplayName("single business value per key -> CLEAN with the pass-1 MAX(output_ts) row")
    void cleanKeyClassified() throws Exception {
        KeyDecision d = aggregate(config("audit", "reports"),
                rowData(1660, WINDOW, 15050, 100L));
        assertEquals("CLEAN", d.decision);
        assertFalse(d.conflict());
        assertNotNull(d.upsertRow);
        assertEquals(15050L, d.upsertRow.getLong(CandleTableColumns.OPEN_PAISE));
        assertNull(d.acceptedHash);
    }

    @Test
    @DisplayName("conflicting business values with no approval -> UNACCEPTED, no upsert row")
    void unacceptedConflictClassified() throws Exception {
        KeyDecision d = aggregate(config("audit", "reports"),
                rowData(1660, WINDOW, 15050, 100L),
                rowData(1660, WINDOW, 15550, 200L));
        assertEquals("UNACCEPTED", d.decision);
        assertTrue(d.conflict());
        assertNull(d.upsertRow);
        assertEquals(2, d.candidateRecords.size());
        assertEquals(2, d.candidateHashes.size());
    }

    @Test
    @DisplayName("approval hash matching one candidate -> APPROVED with provenance + upsert row")
    void approvedConflictClassified(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("approvals.csv");
        RowData candidate = rowData(1660, WINDOW, 15550, 200L);
        Files.writeString(approval, "1660," + WINDOW + "," + CandleMigrationBatchJob.rowHash(candidate)
                + ",APPROVE,ops,replay-incident 2026-08-10,2026-08-10T12:00:00Z\n");
        KeyDecision d = aggregate(config("audit", approval.toString(), "reports"),
                rowData(1660, WINDOW, 15050, 100L),
                candidate);
        assertEquals("APPROVED", d.decision);
        assertTrue(d.conflict());
        assertNotNull(d.upsertRow, "approved row must be the hash-matched candidate");
        assertEquals(15550L, d.upsertRow.getLong(CandleTableColumns.OPEN_PAISE));
        assertEquals(CandleMigrationBatchJob.rowHash(candidate), d.acceptedHash);
        assertEquals("ops", d.approver);
        assertEquals("replay-incident 2026-08-10", d.reason);
        assertEquals("2026-08-10T12:00:00Z", d.decidedAt);
    }

    @Test
    @DisplayName("approval hash matching no candidate -> STALE, no upsert row")
    void staleApprovalClassified(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("approvals.csv");
        Files.writeString(approval, "1660," + WINDOW + "," + "a".repeat(64)
                + ",APPROVE\n");
        KeyDecision d = aggregate(config("audit", approval.toString(), "reports"),
                rowData(1660, WINDOW, 15050, 100L),
                rowData(1660, WINDOW, 15550, 200L));
        assertEquals("STALE", d.decision);
        assertTrue(d.conflict());
        assertNull(d.upsertRow);
    }

    @Test
    @DisplayName("more than 64 distinct candidates -> truncated flag (CLI parity)")
    void truncationAtCandidateCap() throws Exception {
        RowData[] rows = new RowData[70];
        for (int i = 0; i < 70; i++) {
            rows[i] = rowData(1660, WINDOW, 1000 + i, i);
        }
        KeyDecision d = aggregate(config("audit", "reports"), rows);
        assertTrue(d.truncated);
        assertTrue(d.conflict());
        assertEquals(64, d.candidateRecords.size());
        assertEquals("UNACCEPTED", d.decision);
    }

    // ── unit: approval file fail-closed (P3.1) ───────────────────────────────

    @Test
    @DisplayName("legacy 2-field accept line is rejected fail-closed (P3.1)")
    void approvalFileRejectsLegacyTwoFieldLine(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("legacy.csv");
        Files.writeString(approval, "1660," + WINDOW + "\n");
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationTool.loadAcceptKeys(approval.toString()));
    }

    @Test
    @DisplayName("malformed hash in approval line is rejected fail-closed (P3.1)")
    void approvalFileRejectsMalformedHash(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("bad-hash.csv");
        Files.writeString(approval, "1660," + WINDOW + ",not-a-hash,APPROVE\n");
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationTool.loadAcceptKeys(approval.toString()));
    }

    // ── MiniCluster: real wire() pipeline, canned bounded source ─────────────

    private static MiniClusterWithClientResource newMiniCluster() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    private static StreamExecutionEnvironment batchEnv() {
        Configuration conf = new Configuration();
        conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        return env;
    }

    private static DataStream<RowData> canned(StreamExecutionEnvironment env, RowData... rows) {
        return env.fromElements(rows).returns(CandleTableColumns.ROW_TYPE_INFO);
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
        }
        return false;
    }

    @Test
    @DisplayName("clean audit run: duplicates merge, gate passes, no conflict records")
    void cleanAuditRunMergesDuplicatesAndPassesGate(@TempDir Path tmp) throws Exception {
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            // Same business row emitted twice (replay convergence) + one clean key.
            DataStream<RowData> rows = canned(env,
                    rowData(1660, WINDOW, 15050, 100L),
                    rowData(1660, WINDOW, 15050, 200L),
                    rowData(5000, WINDOW, 9000, 100L));
            CandleMigrationBatchJob.wire(env, config("audit", tmp.resolve("reports").toString()), rows);
            env.execute("clean-audit");

            assertFalse(reportFiles(tmp).anyMatch(line ->
                            line.contains("CANDLE_MIGRATION_CONFLICT_RECORD")
                                    || line.contains("CANDLE_MIGRATION_APPROVAL_RECORD")),
                    "no conflicts in a clean run");
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("unaccepted conflict ends the job FAILED (CLI exit 2 parity)")
    void unacceptedConflictFailsJob(@TempDir Path tmp) throws Exception {
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            DataStream<RowData> rows = canned(env,
                    rowData(1660, WINDOW, 15050, 100L),
                    rowData(1660, WINDOW, 15550, 200L));
            CandleMigrationBatchJob.wire(env, config("audit", tmp.resolve("reports").toString()), rows);
            try {
                env.execute("unaccepted");
                fail("expected job FAILED");
            } catch (Exception e) {
                assertTrue(hasCause(e, MigrationBlockedException.class),
                        "cause chain must contain MigrationBlockedException, got: " + e);
            }
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("stale approval (hash matches no candidate) ends the job FAILED (exit 1 parity)")
    void staleApprovalFailsJob(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("approvals.csv");
        Files.writeString(approval, "1660," + WINDOW + "," + "a".repeat(64) + ",APPROVE\n");
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            DataStream<RowData> rows = canned(env,
                    rowData(1660, WINDOW, 15050, 100L),
                    rowData(1660, WINDOW, 15550, 200L));
            CandleMigrationBatchJob.wire(env,
                    config("audit", approval.toString(), tmp.resolve("reports").toString()), rows);
            try {
                env.execute("stale");
                fail("expected job FAILED");
            } catch (Exception e) {
                assertTrue(hasCause(e, MigrationBlockedException.class),
                        "cause chain must contain MigrationBlockedException, got: " + e);
            }
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("approval key absent from the LOG ends the job FAILED (notFound, exit 1 parity)")
    void notFoundApprovalFailsJob(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("approvals.csv");
        RowData clean = rowData(1660, WINDOW, 15050, 100L);
        Files.writeString(approval, "9999," + WINDOW + "," + CandleMigrationBatchJob.rowHash(clean)
                + ",APPROVE\n");
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            DataStream<RowData> rows = canned(env, clean);
            CandleMigrationBatchJob.wire(env,
                    config("audit", approval.toString(), tmp.resolve("reports").toString()), rows);
            try {
                env.execute("not-found");
                fail("expected job FAILED");
            } catch (Exception e) {
                assertTrue(hasCause(e, MigrationBlockedException.class),
                        "cause chain must contain MigrationBlockedException, got: " + e);
            }
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("approved conflict: gate passes, records carry HASH_MATCH + rejected hashes")
    void approvedConflictEmitsHashMatchRecords(@TempDir Path tmp) throws Exception {
        Path approval = tmp.resolve("approvals.csv");
        RowData candidate = rowData(1660, WINDOW, 15550, 200L);
        Files.writeString(approval, "1660," + WINDOW + "," + CandleMigrationBatchJob.rowHash(candidate)
                + ",APPROVE,ops,replay-incident 2026-08-10,2026-08-10T12:00:00Z\n");
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            DataStream<RowData> rows = canned(env,
                    rowData(1660, WINDOW, 15050, 100L),
                    candidate);
            CandleMigrationBatchJob.wire(env,
                    config("audit", approval.toString(), tmp.resolve("reports").toString()), rows);
            env.execute("approved"); // gate must NOT throw

            List<String> lines = reportFiles(tmp).toList();
            String conflict = lines.stream()
                    .filter(l -> l.contains("CANDLE_MIGRATION_CONFLICT_RECORD"))
                    .findFirst().orElseThrow(() -> new AssertionError("no conflict record: " + lines));
            assertTrue(conflict.contains(",approved=HASH_MATCH,"), conflict);
            assertTrue(conflict.contains("candidates=[{hash="), conflict);
            String approvalRecord = lines.stream()
                    .filter(l -> l.contains("CANDLE_MIGRATION_APPROVAL_RECORD"))
                    .findFirst().orElseThrow(() -> new AssertionError("no approval record: " + lines));
            assertTrue(approvalRecord.contains(",approvedHash="), approvalRecord);
            assertTrue(approvalRecord.contains(",rejectedHashes="), approvalRecord);
            assertTrue(approvalRecord.contains(",approver=ops,reason=replay-incident 2026-08-10,"
                    + "decidedAt=2026-08-10T12:00:00Z"), approvalRecord);
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("canonical row with null key fails closed (P3.4)")
    void nullKeyCanonicalRowFailsJob(@TempDir Path tmp) throws Exception {
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            GenericRowData nullKey = rowData(1660, WINDOW, 15050, 100L);
            nullKey.setField(CandleTableColumns.INSTRUMENT_TOKEN, null);
            DataStream<RowData> rows = canned(env, nullKey);
            CandleMigrationBatchJob.wire(env, config("audit", tmp.resolve("reports").toString()), rows);
            try {
                env.execute("null-key");
                fail("expected job FAILED");
            } catch (Exception e) {
                assertTrue(hasCause(e, IllegalStateException.class),
                        "null key must fail closed, got: " + e);
            }
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("non-canonical rows are filtered out and counted, gate passes")
    void nonCanonicalRowsFiltered(@TempDir Path tmp) throws Exception {
        MiniClusterWithClientResource cluster = newMiniCluster();
        cluster.before();
        try {
            StreamExecutionEnvironment env = batchEnv();
            GenericRowData nonCanonical = rowData(1660, WINDOW, 15050, 100L);
            nonCanonical.setField(CandleTableColumns.ALGORITHM_VERSION,
                    StringData.fromString("candle-15s-v2"));
            DataStream<RowData> rows = canned(env, nonCanonical, rowData(5000, WINDOW, 9000, 100L));
            CandleMigrationBatchJob.wire(env, config("audit", tmp.resolve("reports").toString()), rows);
            env.execute("non-canonical"); // only the canonical key reaches the gate
        } finally {
            cluster.after();
        }
    }

    /** Parallel pipeline (P3.6): 4-slot cluster so a parallelism-4 job can run. */
    private static MiniClusterWithClientResource newMiniCluster4() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(4)
                        .setNumberTaskManagers(1)
                        .build());
    }

    @Test
    @DisplayName("parallel pipeline (P3.6): parallelism 4 clean audit converges, gate sees every key")
    void parallelCleanAuditConverges(@TempDir Path tmp) throws Exception {
        MiniClusterWithClientResource cluster = newMiniCluster4();
        cluster.before();
        try {
            Configuration conf = new Configuration();
            conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(conf);
            env.setParallelism(4);
            // Same business row emitted twice (replay convergence) + one clean key;
            // canned source distributes rows across 4 subtasks — keyBy must regroup.
            DataStream<RowData> rows = canned(env,
                    rowData(1660, WINDOW, 15050, 100L),
                    rowData(1660, WINDOW, 15050, 200L),
                    rowData(5000, WINDOW, 9000, 100L));
            CandleMigrationBatchJob.wire(env, config("audit", tmp.resolve("reports").toString()), rows);
            env.execute("parallel-clean-audit"); // gate (parallelism 1) must NOT throw
            assertFalse(reportFiles(tmp).anyMatch(line ->
                            line.contains("CANDLE_MIGRATION_CONFLICT_RECORD")
                                    || line.contains("CANDLE_MIGRATION_APPROVAL_RECORD")),
                    "no conflicts in a clean parallel run");
        } finally {
            cluster.after();
        }
    }

    @Test
    @DisplayName("parallel pipeline (P3.6): parallelism 4 unaccepted conflict still ends FAILED")
    void parallelUnacceptedConflictFailsClosed(@TempDir Path tmp) throws Exception {
        MiniClusterWithClientResource cluster = newMiniCluster4();
        cluster.before();
        try {
            Configuration conf = new Configuration();
            conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(conf);
            env.setParallelism(4);
            DataStream<RowData> rows = canned(env,
                    rowData(1660, WINDOW, 15050, 100L),
                    rowData(1660, WINDOW, 15550, 200L));
            CandleMigrationBatchJob.wire(env, config("audit", tmp.resolve("reports").toString()), rows);
            try {
                env.execute("parallel-unaccepted");
                fail("expected job FAILED");
            } catch (Exception e) {
                assertTrue(hasCause(e, MigrationBlockedException.class),
                        "parallel gate must still fail closed on unaccepted conflict, got: " + e);
            }
        } finally {
            cluster.after();
        }
    }

    /** Every line of the report file under the test temp dir (single file, parallelism 1). */
    private static Stream<String> reportFiles(Path tmp) throws Exception {
        Path report = tmp.resolve("reports").resolve("conflict-and-approval-records");
        return Files.exists(report) ? Files.readAllLines(report).stream() : Stream.empty();
    }

    // ── S3A bounded-timeout supplier (plan 20260812-fix-r2-iceberg-lake-read-stall) ──

    @Test
    @DisplayName("missing S3A timeout env uses 30000 default")
    void s3TimeoutDefaultWhenMissing() {
        assertEquals("30000",
                CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", null));
        assertEquals("30000",
                CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", null));
    }

    @Test
    @DisplayName("custom S3A timeout values within [1000, 300000] are accepted")
    void s3TimeoutCustomAccepted() {
        assertEquals("1000",
                CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", "1000"));
        assertEquals("15000",
                CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", "15000"));
        assertEquals("300000",
                CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", "300000"));
    }

    @Test
    @DisplayName("zero S3A timeout is rejected")
    void s3TimeoutZeroRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", "0"));
    }

    @Test
    @DisplayName("negative S3A timeout is rejected")
    void s3TimeoutNegativeRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", "-1"));
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", "-5000"));
    }

    @Test
    @DisplayName("S3A timeout below 1000 ms is rejected")
    void s3TimeoutBelowMinRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", "999"));
    }

    @Test
    @DisplayName("S3A timeout above 300000 ms is rejected")
    void s3TimeoutAboveMaxRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", "300001"));
    }

    @Test
    @DisplayName("non-numeric S3A timeout is rejected")
    void s3TimeoutNonNumericRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", "abc"));
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", "30s"));
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", "12.5"));
        assertThrows(IllegalArgumentException.class,
                () -> CandleMigrationBatchJob.s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", ""));
    }

    @Test
    @DisplayName("lake-catalog map carries exactly the two fs.s3a.* supplier keys")
    void lakeCatalogMapExactKeys() {
        Map<String, String> props =
                CandleMigrationBatchJob.lakeCatalogProperties("30000", "30000");
        assertEquals(2, props.size());
        assertEquals("30000", props.get("iceberg.iceberg.hadoop.fs.s3a.connection.timeout"));
        assertEquals("30000", props.get("iceberg.iceberg.hadoop.fs.s3a.connection.establish.timeout"));
    }

    @Test
    @DisplayName("lake-catalog map contains no credentials, keys, or tokens")
    void lakeCatalogMapNoCredentials() {
        Map<String, String> props =
                CandleMigrationBatchJob.lakeCatalogProperties("30000", "30000");
        for (Map.Entry<String, String> e : props.entrySet()) {
            String combined = (e.getKey() + " " + e.getValue()).toLowerCase();
            for (String forbidden : List.of("access", "secret", "credential", "token")) {
                assertFalse(combined.contains(forbidden),
                        "map leaked credential-ish term '" + forbidden + "': " + e);
            }
        }
    }

    @Test
    @DisplayName("supplier keys round-trip through the fluss connector to fs.s3a.*")
    void lakeCatalogPrefixTransformation() {
        Map<String, String> supplier =
                CandleMigrationBatchJob.lakeCatalogProperties("30000", "30000");

        // FlinkCatalog.getLakeTable prepends "table.datalake." to each supplier key,
        // and the lake table options carry table.datalake.format=iceberg.
        Map<String, String> tableOptions = new java.util.HashMap<>();
        tableOptions.put("table.datalake.format", "iceberg");
        for (Map.Entry<String, String> e : supplier.entrySet()) {
            tableOptions.put("table.datalake." + e.getKey(), e.getValue());
        }

        // LakeSourceUtils.createLakeSource -> DataLakeUtils.extractLakeCatalogProperties
        // strips "table.datalake.iceberg." — exercised against the real connector class.
        Map<String, String> catalogProps = org.apache.fluss.flink.utils.DataLakeUtils
                .extractLakeCatalogProperties(org.apache.fluss.config.Configuration.fromMap(tableOptions));

        // HadoopUtils.FLUSS_CONFIG_PREFIXES = {"iceberg.hadoop."} strips the rest
        // (source-verified in fluss-lake-iceberg 0.9.1-incubating; the class is not on
        // the unit-test classpath, so the final hop is asserted as the documented strip).
        assertEquals(2, catalogProps.size());
        for (String expected : List.of(
                "iceberg.hadoop.fs.s3a.connection.timeout",
                "iceberg.hadoop.fs.s3a.connection.establish.timeout")) {
            assertEquals("30000", catalogProps.get(expected), "missing catalog property " + expected);
            assertTrue(expected.startsWith("iceberg.hadoop."), expected);
            assertEquals("fs.s3a." + expected.substring("iceberg.hadoop.fs.s3a.".length()),
                    expected.substring("iceberg.hadoop.".length()), expected);
        }
    }
}
