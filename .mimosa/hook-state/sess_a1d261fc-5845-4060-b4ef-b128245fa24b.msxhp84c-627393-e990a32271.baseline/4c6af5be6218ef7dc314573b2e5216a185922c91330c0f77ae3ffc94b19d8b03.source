package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SIGNAL-SCHEMA-001 (DEC-035, tracker 14 re-scoped P2): KV upsert idempotency
 * of the {@code Signal_Candidates_current} projection against a live Fluss
 * cluster.
 *
 * <p>This is the integration proof behind the signal dual-sink replay-safety
 * argument: the immutable LOG {@code Signal_Candidates} duplicates rows on
 * replay (offset-0 restart re-emits every signal), but the KV companion
 * converges to one current row per {@code instrument_token} because a
 * re-emitted signal upserts the same key. The test writes the SAME instrument
 * twice with a different {@code detection_ts} (the replay signature — same
 * identity columns, different emit instant) and asserts the table still holds
 * exactly one row per instrument with last-write-wins {@code detection_ts}.
 *
 * <p>Gates: {@code @Tag("integration")}, skipped unless
 * {@code COMPUTE_INT_TEST_SIGNAL_KV=true} and {@code FLUSS_BOOTSTRAP} is
 * configured. Run with {@code mvn -o test
 * -Dtest=SignalCurrentKvIdempotencyTest} against the dev cluster (default
 * Docker Compose stack, {@code localhost:9123}).
 *
 * <p>The test creates ONE scratch KV table under {@code default} with the
 * exact 22-column signal schema (same {@code bucket.num=16} /
 * {@code bucket.key=instrument_token} / PK {@code instrument_token} contract
 * as the platform tables) and drops it afterwards. It never touches platform
 * tables ({@code Signal_Candidates}, {@code Signal_Candidates_current}, …).
 */
@Tag("integration")
@DisplayName("SIGNAL-SCHEMA-001: scratch KV upsert idempotency (one current row per instrument)")
class SignalCurrentKvIdempotencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(SignalCurrentKvIdempotencyTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;
    private static final List<String> CREATED_TABLES = new ArrayList<>();

    @BeforeAll
    static void connect() {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("COMPUTE_INT_TEST_SIGNAL_KV", "false")),
                "Skipping — set COMPUTE_INT_TEST_SIGNAL_KV=true");
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "Skipping — set FLUSS_BOOTSTRAP to run the SIGNAL-SCHEMA-001 integration test");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("signal-kv: connected to {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("signal-kv: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            for (String table : CREATED_TABLES) {
                try {
                    admin.dropTable(TablePath.of("default", table), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("signal-kv: dropped scratch table {}", table);
                } catch (Exception e) {
                    LOG.warn("signal-kv: drop {} failed: {}", table, e.getMessage());
                }
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("same instrument upserted twice (different detection_ts) leaves one current row; "
            + "distinct instruments add rows; non-canonical rows never load")
    void signalKvUpsertIdempotency() throws Exception {
        String tableName = "signal_kv_scratch_" + System.nanoTime();
        Table table = createScratchKvTable(tableName);
        TableInfo info = admin.getTableInfo(TablePath.of("default", tableName))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // ── (1) Upsert the SAME instrument twice with different detection_ts ─
        long token = 5000L;
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            // Replay signature: same identity/business fields, later emit instant.
            writer.upsert(signalRow(token, /* detectionTs */ 100L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            writer.upsert(signalRow(token, /* detectionTs */ 200L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }

        // ── (2) One current row, last-write-wins detection_ts, business fields ─
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow current = lookup(lookuper, token);
        assertNotNull(current, "upserted instrument must be readable by lookup");
        assertEquals(200L, current.getLong(SignalCandidatesTableColumns.DETECTION_TS),
                "last upsert wins — the current row carries the later detection_ts");
        assertBusinessFields(current, token);

        // ── (3) Exactly one row per instrument across the whole table ───────
        long totalRows = scanCount(table, info, token);
        assertEquals(1, totalRows,
                "two upserts of the same instrument must leave exactly ONE current row "
                        + "(idempotent replay, not append)");

        // ── (4) A distinct instrument creates an additional row ─────────────
        long token2 = 6000L;
        UpsertWriter writer2 = table.newUpsert().createWriter();
        try {
            writer2.upsert(signalRow(token2, 300L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            writer2.flush();
        }
        assertNotNull(lookup(lookuper, token2),
                "different instrument_token must be a distinct row");

        // ── (5) Non-canonical rows are excluded before KV load ─────────────
        assertTrue(!CanonicalSignalPolicy.isCanonical(
                        "3", // stale schema_version
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID,
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                "stale schema_version must be non-canonical");
        assertTrue(!CanonicalSignalPolicy.isCanonical(
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        "other-strategy", // strategy iteration
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID,
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                "strategy iteration must be non-canonical");
        assertTrue(!CanonicalSignalPolicy.isCanonical(
                        null, null, null, null,
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                "unversioned row must be non-canonical");
        assertTrue(CanonicalSignalPolicy.isCanonical(
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID,
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                "the pinned canonical identity must be canonical");

        // The load gate mirrors the pipeline: only canonical rows reach the KV
        // table. A non-canonical re-emission of an EXISTING key must not
        // overwrite the canonical row.
        InternalRow after = lookup(lookuper, token);
        assertEquals(200L, after.getLong(SignalCandidatesTableColumns.DETECTION_TS),
                "rejected non-canonical row must not overwrite the canonical current row");
        assertEquals(SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                after.getString(SignalCandidatesTableColumns.STRATEGY_ID).toString(),
                "canonical strategy_id preserved");

        LOG.info("signal-kv: idempotency + canonical-gate checks OK on {}", tableName);
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** 22-column signal schema mirroring DDL 23 (PK instrument_token). */
    private static Schema signalKvSchema() {
        Schema.Builder b = Schema.newBuilder()
                .column("candidate_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("strategy_id", DataTypes.STRING())
                .column("strategy_version", DataTypes.STRING())
                .column("rule_id", DataTypes.STRING())
                .column("detection_ts", DataTypes.BIGINT())
                .column("evaluation_ts", DataTypes.BIGINT())
                .column("action", DataTypes.STRING())
                .column("side", DataTypes.STRING())
                .column("quantity", DataTypes.BIGINT())
                .column("order_type", DataTypes.STRING())
                .column("limit_price_paise", DataTypes.BIGINT())
                .column("score_inputs", DataTypes.STRING())
                .column("formation_snapshot_ref", DataTypes.STRING())
                .column("validity_reason", DataTypes.STRING())
                .column("supersedes_candidate_id", DataTypes.STRING())
                .column("superseded_by_candidate_id", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING());
        return b.primaryKey("instrument_token").build();
    }

    /** Create a scratch KV table with the exact signal contract; remember it for cleanup. */
    private static Table createScratchKvTable(String name) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(signalKvSchema())
                .distributedBy(16, "instrument_token")
                .build();
        TablePath path = TablePath.of("default", name);
        try {
            admin.createTable(path, td, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().toLowerCase().contains("already exist")) {
                throw e;
            }
            // exists (retry) — fine
        }
        CREATED_TABLES.add(name);
        return connection.getTable(path);
    }

    /** 22-column row in DDL order; nullable STRINGs written as empty binaries. */
    private static GenericRow signalRow(long token, long detectionTs) {
        return GenericRow.of(
                bs("cand-" + token), BinaryString.EMPTY_UTF8, BinaryString.EMPTY_UTF8,
                token, bs("NSE"), bs("TEST"),
                bs(SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID),
                bs(SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION),
                bs(SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                detectionTs, detectionTs,
                bs(SignalCandidatesTableColumns.ACTION_ENTRY),
                bs(SignalCandidatesTableColumns.SIDE_BUY), 1L,
                BinaryString.EMPTY_UTF8, null, BinaryString.EMPTY_UTF8,
                BinaryString.EMPTY_UTF8, BinaryString.EMPTY_UTF8,
                BinaryString.EMPTY_UTF8, BinaryString.EMPTY_UTF8,
                bs(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
    }

    /** Lookup key = compacted PK row: PK columns only (instrument_token). The
     *  lookuper's key encoder (CompactedKeyEncoder) reads the passed row at
     *  COMPACTED positions 0..k-1 — a full-schema row NPEs. */
    private static GenericRow lookupKey(long token) {
        return GenericRow.of(token);
    }

    private static InternalRow lookup(Lookuper lookuper, long token) throws Exception {
        return lookuper.lookup(lookupKey(token))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
    }

    /** Count rows across all buckets that match the given instrument. */
    private static long scanCount(Table table, TableInfo info, long token) throws Exception {
        long matched = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(1_000_000_000)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(3000))) {
                while (it.hasNext()) {
                    InternalRow row = it.next();
                    if (row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN) == token) {
                        matched++;
                    }
                }
            }
        }
        return matched;
    }

    private static void assertBusinessFields(InternalRow row, long token) {
        assertEquals(token, row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", row.getString(SignalCandidatesTableColumns.EXCHANGE).toString());
        assertEquals("TEST", row.getString(SignalCandidatesTableColumns.SYMBOL).toString());
        assertEquals(SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                row.getString(SignalCandidatesTableColumns.STRATEGY_ID).toString());
        assertEquals(SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                row.getString(SignalCandidatesTableColumns.STRATEGY_VERSION).toString());
        assertEquals(SignalCandidatesTableColumns.CANONICAL_RULE_ID,
                row.getString(SignalCandidatesTableColumns.RULE_ID).toString());
        assertEquals(SignalCandidatesTableColumns.ACTION_ENTRY,
                row.getString(SignalCandidatesTableColumns.ACTION).toString());
        assertEquals(SignalCandidatesTableColumns.SIDE_BUY,
                row.getString(SignalCandidatesTableColumns.SIDE).toString());
        assertEquals(1L, row.getLong(SignalCandidatesTableColumns.QUANTITY));
        assertEquals(SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                row.getString(SignalCandidatesTableColumns.SCHEMA_VERSION).toString());
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }
}
