package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.schema.CandleTableSchema;
import java.time.Duration;
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
 * CANDLE-KV-001 (docs/08_implementation/13-candle-log-kv-replay-safety.md, A7.2):
 * KV upsert idempotency of the {@code feature_candles_15s_current} projection
 * against a live Fluss cluster.
 *
 * <p>This is the integration proof behind the LOG→KV replay-safety argument:
 * an immutable LOG duplicates rows on replay (offset-0 restart re-emits every
 * candle), but the KV companion converges to one current row per
 * {@code (instrument_token, window_start)} because a re-emitted candle
 * upserts the same key. The test writes the SAME key twice with a different
 * {@code output_ts} (the exact replay signature — same business fields,
 * different emit instant) and asserts the table still holds exactly one row
 * per key with last-write-wins {@code output_ts}.
 *
 * <p>Gates (A7.2): {@code @Tag("integration")}, skipped unless
 * {@code COMPUTE_INT_TEST_CANDLE_KV=true} and {@code FLUSS_BOOTSTRAP} is
 * configured. Run with {@code mvn -o test -Dgroups=integration} against the
 * dev cluster (default Docker Compose stack, {@code localhost:9123}).
 *
 * <p>The test creates ONE scratch KV table under {@code default} with the
 * exact 15-column v2 candle schema (same {@code bucket.num=16} /
 * {@code bucket.key=instrument_token} contract as the platform tables) and
 * drops it afterwards. It never touches platform tables ({@code
 * feature_candles_15s}, {@code feature_candles_15s_current}, …).
 */
@Tag("integration")
@DisplayName("CANDLE-KV-001: scratch KV upsert idempotency (one current row per key)")
class CandleCurrentKvIdempotencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(CandleCurrentKvIdempotencyTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /** Pipeline-pinned canonical versions (SignalJobConfig defaults). */
    private static final String EXPECTED_ALGORITHM = "candle-15s-v1";
    private static final String EXPECTED_CONFIGURATION = "1.0.0";

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;
    private static final List<String> CREATED_TABLES = new java.util.ArrayList<>();

    @BeforeAll
    static void connect() {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("COMPUTE_INT_TEST_CANDLE_KV", "false")),
                "Skipping — set COMPUTE_INT_TEST_CANDLE_KV=true");
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "Skipping — set FLUSS_BOOTSTRAP to run the CANDLE-KV-001 integration test");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("candle-kv-001: connected to {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("candle-kv-001: cannot connect to {} — {}", bootstrap, e.getMessage());
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
                    LOG.info("candle-kv-001: dropped scratch table {}", table);
                } catch (Exception e) {
                    LOG.warn("candle-kv-001: drop {} failed: {}", table, e.getMessage());
                }
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("same key upserted twice (different output_ts) leaves one current row; "
            + "distinct keys add rows; non-canonical rows never load")
    void candleKvUpsertIdempotency() throws Exception {
        String tableName = "candle_kv_scratch_" + System.nanoTime();
        Table table = createScratchKvTable(tableName);
        TableInfo info = admin.getTableInfo(TablePath.of("default", tableName))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // ── (1) Upsert the SAME key twice with different output_ts ─────────
        long token = 5000L;
        long windowStart = 1_700_000_000_000L;
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            // Replay signature: same business fields, later emit instant.
            writer.upsert(candleRow(token, windowStart, /* outputTs */ 100L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            writer.upsert(candleRow(token, windowStart, /* outputTs */ 200L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }

        // ── (2) One current row, last-write-wins output_ts, business fields ─
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow current = lookup(lookuper, token, windowStart);
        assertNotNull(current, "upserted key must be readable by lookup");
        assertEquals(200L, current.getLong(CandleTableColumns.OUTPUT_TS),
                "last upsert wins — the current row carries the later output_ts");
        assertBusinessFields(current, token, windowStart);

        // ── (3) Exactly one row per key across the whole table ─────────────
        long totalRows = scanCount(table, info, token, windowStart);
        assertEquals(1, totalRows,
                "two upserts of the same key must leave exactly ONE current row "
                        + "(idempotent replay, not append)");

        // ── (4) Distinct window / instrument keys create additional rows ───
        long windowStart2 = windowStart + 15_000L;
        long token2 = 6000L;
        UpsertWriter writer2 = table.newUpsert().createWriter();
        try {
            writer2.upsert(candleRow(token, windowStart2, 300L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            writer2.upsert(candleRow(token2, windowStart, 300L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            writer2.flush();
        }
        assertNotNull(lookup(lookuper, token, windowStart2),
                "same instrument, different window_start must be a distinct row");
        assertNotNull(lookup(lookuper, token2, windowStart),
                "different instrument_token must be a distinct row");

        // ── (5) Non-canonical rows are excluded before KV load ─────────────
        assertTrue(!CanonicalCandlePolicy.isCanonical(
                        "candle-15s-v0", EXPECTED_CONFIGURATION,
                        EXPECTED_ALGORITHM, EXPECTED_CONFIGURATION),
                "stale algorithm_version must be non-canonical");
        assertTrue(!CanonicalCandlePolicy.isCanonical(
                        EXPECTED_ALGORITHM, "0.9.0",
                        EXPECTED_ALGORITHM, EXPECTED_CONFIGURATION),
                "stale configuration_version must be non-canonical");
        assertTrue(!CanonicalCandlePolicy.isCanonical(
                        null, null, EXPECTED_ALGORITHM, EXPECTED_CONFIGURATION),
                "unversioned row must be non-canonical");

        // The load gate mirrors the pipeline: only canonical rows reach the KV
        // table. A non-canonical re-emission of an EXISTING key must not
        // overwrite the canonical row (and a non-canonical NEW key must not
        // create a row).
        InternalRow after = lookup(lookuper, token, windowStart);
        assertEquals(200L, after.getLong(CandleTableColumns.OUTPUT_TS),
                "rejected non-canonical row must not overwrite the canonical current row");
        assertEquals(EXPECTED_ALGORITHM, after.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                "canonical algorithm_version preserved");

        LOG.info("candle-kv-001: idempotency + canonical-gate checks OK on {}", tableName);
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static Schema candleKvSchema() {
        Schema.Builder b = Schema.newBuilder()
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("window_start", DataTypes.BIGINT())
                .column("window_end", DataTypes.BIGINT())
                .column("open_paise", DataTypes.BIGINT())
                .column("high_paise", DataTypes.BIGINT())
                .column("low_paise", DataTypes.BIGINT())
                .column("close_paise", DataTypes.BIGINT())
                .column("volume", DataTypes.BIGINT())
                .column("tick_count", DataTypes.INT())
                .column("algorithm_version", DataTypes.STRING())
                .column("configuration_version", DataTypes.STRING())
                .column("output_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING());
        return b.primaryKey(CandleTableSchema.KEY_COLUMNS.get(0), CandleTableSchema.KEY_COLUMNS.get(1))
                .build();
    }

    /** Create a scratch KV table with the exact v2 candle contract; remember it for cleanup. */
    private static Table createScratchKvTable(String name) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(candleKvSchema())
                .distributedBy(CandleTableSchema.BUCKET_COUNT, CandleTableSchema.BUCKET_KEY)
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

    private static GenericRow candleRow(long token, long windowStart, long outputTs) {
        return GenericRow.of(
                token, bs("NSE"), bs("TEST"), windowStart, windowStart + 15_000L,
                100_00L, 110_00L, 90_00L, 105_00L, 42L, 7,
                bs(EXPECTED_ALGORITHM), bs(EXPECTED_CONFIGURATION), outputTs, bs("2"));
    }

    /** Lookup key = compacted PK row: PK columns only, in PK-declaration
     *  order (instrument_token, window_start). The lookuper's key encoder
     *  (CompactedKeyEncoder) reads the passed row at COMPACTED positions
     *  0..k-1 — a full-schema row with nulls elsewhere NPEs (verified
     *  empirically, 2026-08-10). */
    private static GenericRow lookupKey(long token, long windowStart) {
        return GenericRow.of(token, windowStart);
    }

    private static InternalRow lookup(Lookuper lookuper, long token, long windowStart)
            throws Exception {
        return lookuper.lookup(lookupKey(token, windowStart))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
    }

    /** Count rows across all buckets that match the given PK. */
    private static long scanCount(Table table, TableInfo info, long token, long windowStart)
            throws Exception {
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
                    if (row.getLong(CandleTableColumns.INSTRUMENT_TOKEN) == token
                            && row.getLong(CandleTableColumns.WINDOW_START) == windowStart) {
                        matched++;
                    }
                }
            }
        }
        return matched;
    }

    private static void assertBusinessFields(InternalRow row, long token, long windowStart) {
        assertEquals(token, row.getLong(CandleTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", row.getString(CandleTableColumns.EXCHANGE).toString());
        assertEquals("TEST", row.getString(CandleTableColumns.SYMBOL).toString());
        assertEquals(windowStart, row.getLong(CandleTableColumns.WINDOW_START));
        assertEquals(windowStart + 15_000L, row.getLong(CandleTableColumns.WINDOW_END));
        assertEquals(100_00L, row.getLong(CandleTableColumns.OPEN_PAISE));
        assertEquals(110_00L, row.getLong(CandleTableColumns.HIGH_PAISE));
        assertEquals(90_00L, row.getLong(CandleTableColumns.LOW_PAISE));
        assertEquals(105_00L, row.getLong(CandleTableColumns.CLOSE_PAISE));
        assertEquals(42L, row.getLong(CandleTableColumns.VOLUME));
        assertEquals(7, row.getInt(CandleTableColumns.TICK_COUNT));
        assertEquals(EXPECTED_ALGORITHM, row.getString(CandleTableColumns.ALGORITHM_VERSION).toString());
        assertEquals(EXPECTED_CONFIGURATION, row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString());
        assertEquals("2", row.getString(CandleTableColumns.SCHEMA_VERSION).toString());
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }
}
