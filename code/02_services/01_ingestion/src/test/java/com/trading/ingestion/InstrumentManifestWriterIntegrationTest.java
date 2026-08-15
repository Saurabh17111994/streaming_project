package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.ingestion.InstrumentManifestWriter.ManifestEntry;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ING-INT-004 / instrument-loader live proof: {@link InstrumentManifestWriter}
 * is the first production composite-PK raw-client writer. Against a live
 * Fluss cluster it upserts a manifest into an {@code instruments}-shaped
 * scratch table (composite PK, single-field subset bucket key,
 * kv.format-version=2) and verifies version retention + idempotent re-load.
 *
 * <p>Env-gated: set {@code INGESTION_INT_TEST_INSTRUMENTS=true} (Fluss at
 * {@code FLUSS_BOOTSTRAP_SERVERS}, default {@code localhost:9123}). The
 * scratch table is dropped after the run; platform tables are never touched.
 */
@DisplayName("ING-INT-004: Instrument manifest writer — composite-PK raw-client upserts")
class InstrumentManifestWriterIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(InstrumentManifestWriterIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    @DisplayName("write manifest to instruments-shaped KV; version retention + idempotent re-load")
    void writeAndReadBackManifest() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_INSTRUMENTS", "false")),
                "Skipping — set INGESTION_INT_TEST_INSTRUMENTS=true");

        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");
        String scratch = "instruments_it_" + System.nanoTime();

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Admin admin = connection.getAdmin();

            // Exact instruments DDL shape (14 columns, composite PK, single-field
            // subset bucket key, kv.format-version=2 — the raw-client composite-PK
            // configuration that COMPAT-FLUSS-005 pins).
            Schema schema = Schema.newBuilder()
                    .column("instrument_token", DataTypes.BIGINT())
                    .column("trading_symbol", DataTypes.STRING())
                    .column("exchange", DataTypes.STRING())
                    .column("segment", DataTypes.STRING())
                    .column("instrument_type", DataTypes.STRING())
                    .column("lot_size", DataTypes.INT())
                    .column("tick_size_paise", DataTypes.BIGINT())
                    .column("strike_paise", DataTypes.BIGINT())
                    .column("expiry", DataTypes.BIGINT())
                    .column("option_type", DataTypes.STRING())
                    .column("manifest_version", DataTypes.INT())
                    .column("is_active", DataTypes.BOOLEAN())
                    .column("loaded_ts", DataTypes.BIGINT())
                    .column("schema_version", DataTypes.STRING())
                    .primaryKey("instrument_token", "manifest_version")
                    .build();
            TableDescriptor td = TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(4, "instrument_token")
                    .property("table.kv.format-version", "2")
                    .build();
            TablePath path = TablePath.of("default", scratch);
            admin.createTable(path, td, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            TableInfo info = admin.getTableInfo(path).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            LOG.info("ing-int-004: scratch table {} created (id={}, buckets={}, PK={}, bucketKeys={})",
                    scratch, info.getTableId(), info.getNumBuckets(),
                    info.getPrimaryKeys(), info.getBucketKeys());

            try {
                // The first production composite-PK raw-client writer — the writer's
                // constructor preflights the composite-PK KV shape itself.
                List<ManifestEntry> manifest = List.of(
                        entry(123, 1, "NIFTY"),
                        entry(123, 2, "NIFTY"),   // prior version retained (R-090)
                        entry(456, 1, "BANKNIFTY"));
                int written;
                try (InstrumentManifestWriter writer = new InstrumentManifestWriter(bootstrap, scratch)) {
                    written = writer.write(manifest);
                }
                assertEquals(3, written, "all manifest rows upserted");

                Table table = connection.getTable(path);
                assertEquals(3, distinctCompositeKeys(table, info), "one row per (token, version)");
                assertManifestRow(table, 123L, 1, "NIFTY");
                assertManifestRow(table, 123L, 2, "NIFTY");
                assertManifestRow(table, 456L, 1, "BANKNIFTY");

                // Idempotent re-load: same manifest again changes nothing — same
                // composite keys upsert in place, prior versions stay.
                try (InstrumentManifestWriter writer = new InstrumentManifestWriter(bootstrap, scratch)) {
                    assertEquals(3, writer.write(manifest), "re-load writes the same 3 rows");
                }
                assertEquals(3, distinctCompositeKeys(table, info),
                        "idempotent re-load must not create new (token, version) rows");
                assertManifestRow(table, 123L, 2, "NIFTY");

                LOG.info("ing-int-004: PASS — composite-PK raw-client upserts, version retention, "
                        + "idempotent re-load (table {})", scratch);
            } finally {
                admin.dropTable(path, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("ing-int-004: dropped scratch table {}", scratch);
            }
        }
    }

    private static ManifestEntry entry(long token, int version, String symbol) {
        return new ManifestEntry(token, symbol, "NSE", "CM", "EQUITY",
                75, 500L, null, null, null, version, true,
                1_700_000_000_000L, "3");
    }

    /** Lookup one composite key and assert its full row content. */
    private static void assertManifestRow(Table table, long token, int version, String symbol)
            throws Exception {
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow row = lookuper.lookup(GenericRow.of(token, version))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
        assertNotNull(row, "composite-PK lookup must find (" + token + ", v" + version + ")");
        assertEquals(symbol, row.getString(1).toString(), "trading_symbol for (" + token + ", v" + version + ")");
        assertEquals("NSE", row.getString(2).toString());
        assertEquals("CM", row.getString(3).toString());
        assertEquals(75, row.getInt(5), "lot_size");
        assertEquals(500L, row.getLong(6), "tick_size_paise");
        assertEquals(version, row.getInt(10), "manifest_version column");
        assertTrue(row.getBoolean(11), "is_active");
        assertEquals("3", row.getString(13).toString(), "schema_version");
    }

    /**
     * Count distinct (instrument_token, manifest_version) pairs across all
     * buckets. Counting distinct pairs (rather than raw rows) keeps the
     * assertion correct whether the scanner returns current-state rows or the
     * append-only changelog.
     */
    private static int distinctCompositeKeys(Table table, TableInfo info) throws Exception {
        Set<String> keys = new HashSet<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    InternalRow row = it.next();
                    keys.add(row.getLong(0) + ":" + row.getInt(10));
                }
            }
        }
        return keys.size();
    }
}
