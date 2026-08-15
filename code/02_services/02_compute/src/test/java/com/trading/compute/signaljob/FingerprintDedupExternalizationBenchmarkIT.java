package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
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
 * SIG-STATE-001/002 + SIG-PERF-001 live-cluster evidence for the DEC-038
 * dedup externalization: the {@code fingerprint_dedup} authoritative state
 * table exercised through the PRODUCTION store
 * ({@link FlussFingerprintDedupStateStore}) against a live Fluss cluster.
 *
 * <p>Phases on a scratch {@code fingerprint_dedup}-shaped KV table
 * (composite PK {@code (instrument_token, fingerprint_version,
 * event_fingerprint)}, 16 buckets, {@code bucket.key = instrument_token},
 * {@code kv.format-version = 2} — the COMPAT-FLUSS-005 combo the raw-client
 * writer proves):
 *
 * <ol>
 *   <li><b>Write phase</b> — bulk fill of N live first-seen upserts (expiry =
 *       now + TTL) + M already-expired rows through ONE shared
 *       {@code UpsertWriter} (the same raw-client upsert machinery and row
 *       layout the store uses) measuring raw durable-write throughput
 *       (SIG-PERF-001 write row), PLUS a bounded
 *       {@link FlussFingerprintDedupStateStore#putFirstSeen} subset through
 *       the exact production store path with per-row latency recorded.</li>
 *   <li><b>Sizing</b> — full-table scan count must equal live + stale
 *       (steady-state entries = accepted rate × TTL horizon).</li>
 *   <li><b>Cold-restart hydration</b> — close the store, reopen a fresh one
 *       (simulated restart), sample lookups must all return {@code SEEN_LIVE}
 *       from Fluss authority — a cold cache is correct, never an empty dedup
 *       set (SIG-STATE-002/003); measures lookup latency and the bucket-read
 *       (log-scan vs current-state) duration that is evidence-gated.</li>
 *   <li><b>Bounded cleanup</b> — re-entrant {@code scanExpired} +
 *       {@code delete} passes drain every expired row (delete/ack + bucket-
 *       scoping evidence — SIG-STATE-001); deleted keys return
 *       {@code NOT_SEEN} on the authoritative read path; live rows survive;
 *       final plateau = live count only. This phase is the regression guard
 *       for the 2026-08-15 {@code bucketOf} fix: with the pre-fix
 *       {@code token % numBuckets} scoping, {@code scanExpired} read the wrong
 *       bucket and deleted nothing.</li>
 * </ol>
 *
 * <p>Gates: {@code @Tag("integration")}, skipped unless
 * {@code COMPUTE_INT_TEST_DEDUP_EXT=true}; Fluss at {@code FLUSS_BOOTSTRAP}
 * (default {@code localhost:9123}). Row counts are env-overridable
 * ({@code DEDUP_EXT_LIVE_ROWS} / {@code DEDUP_EXT_STALE_ROWS}); the defaults
 * are the quick smoke (the dev cluster acks every durable write at ~100 ms/row),
 * and the full envelope benchmark is an env-scaled long run per the repo's
 * smoke-then-long-run gate. Run against the dev cluster:
 * {@code COMPUTE_INT_TEST_DEDUP_EXT=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=FingerprintDedupExternalizationBenchmarkIT}.
 *
 * <p>Only ONE scratch table is created and dropped afterwards; platform
 * tables are never touched.
 */
@Tag("integration")
@DisplayName("SIG-STATE-001/002 + SIG-PERF-001: fingerprint_dedup externalization benchmark (live Fluss)")
class FingerprintDedupExternalizationBenchmarkIT {

    private static final Logger LOG = LoggerFactory.getLogger(FingerprintDedupExternalizationBenchmarkIT.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final long TTL_MS = 300_000L; // DEDUP_TTL_MS pin (MVP)
    private static final String VERSION = FingerprintDedupTableColumns.SCHEMA_VERSION_V1;
    private static final int CLEANUP_BATCH = 100;

    private static final long LIVE_BASE_TOKEN = 5_000L;   // buckets 8..13 for 6 tokens
    private static final long STALE_BASE_TOKEN = 7_000L;  // buckets 8..10 for 3 tokens
    private static final long STORE_PATH_TOKEN = 8_000L;  // bucket 0 — distinct from live/stale
    private static final int LIVE_TOKENS = 6;
    private static final int STALE_TOKENS = 3;

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;
    private static final List<String> CREATED_TABLES = new ArrayList<>();

    // Scale (env-overridable; defaults are the quick smoke — the dev cluster
    // acks every durable write (~100 ms/row), so the full envelope benchmark
    // runs with DEDUP_EXT_LIVE_ROWS/STALE_ROWS scaled up per the repo's
    // long-run gate (smoke first, then the long run).
    private static final int LIVE_ROWS =
            intEnv("DEDUP_EXT_LIVE_ROWS", 8);    // per token → 48 live total
    private static final int STALE_ROWS =
            intEnv("DEDUP_EXT_STALE_ROWS", 5);   // per token → 15 stale total
    private static final int STORE_PATH_ROWS =
            intEnv("DEDUP_EXT_STORE_PATH_ROWS", 10);
    private static final int HYDRATION_SAMPLE =
            intEnv("DEDUP_EXT_HYDRATION_SAMPLE", 15);
    private static final int POST_DELETE_VERIFY =
            intEnv("DEDUP_EXT_POST_DELETE_VERIFY", 10);
    private static final long SETTLE_MS =
            longEnv("DEDUP_EXT_SETTLE_MS", 5_000L);

    @BeforeAll
    static void connect() {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("COMPUTE_INT_TEST_DEDUP_EXT", "false")),
                "Skipping — set COMPUTE_INT_TEST_DEDUP_EXT=true");
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("sig-ext: connected to {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("sig-ext: cannot connect to {} — {}", bootstrap, e.getMessage());
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
                    LOG.info("sig-ext: dropped scratch table {}", table);
                } catch (Exception e) {
                    LOG.warn("sig-ext: drop {} failed: {}", table, e.getMessage());
                }
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("write → size → cold-restart hydration → bounded cleanup on the fingerprint_dedup shape")
    void externalizationBenchmark() throws Exception {
        String tableName = "fingerprint_dedup_scratch_" + System.nanoTime();
        Table scratchTable = createScratchDedupTable(tableName);
        TableInfo info = admin.getTableInfo(TablePath.of("default", tableName))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("sig-ext: scratch table {} created (id={}, buckets={}, PK={}, bucketKeys={})",
                tableName, info.getTableId(), info.getNumBuckets(),
                info.getPrimaryKeys(), info.getBucketKeys());
        assertEquals(16, info.getNumBuckets(), "fingerprint_dedup DDL pins 16 buckets");
        assertEquals(List.of("instrument_token"), info.getBucketKeys(),
                "bucket.key must be instrument_token (PK prefix)");

        // Freshly created buckets need leader election to settle before the
        // first write — the raw client otherwise stalls retrying
        // NOT_LEADER_OR_FOLLOWER (observed live; duration is evidence-gated).
        Thread.sleep(SETTLE_MS);
        LOG.info("sig-ext: leader-election settle {} ms elapsed", SETTLE_MS);

        long nowMs = System.currentTimeMillis();

        // ── Phase 1a: bulk durable fill through ONE shared UpsertWriter ───
        //    (the same raw-client upsert machinery and row layout the store
        //    writes; measures raw durable throughput at the write rate).
        List<String> liveKeys = new ArrayList<>();
        List<String> staleKeys = new ArrayList<>();
        long bulkStart = System.nanoTime();
        UpsertWriter bulk = scratchTable.newUpsert().createWriter();
        try {
            for (int t = 0; t < LIVE_TOKENS; t++) {
                long token = LIVE_BASE_TOKEN + t;
                for (int i = 0; i < LIVE_ROWS; i++) {
                    String fp = "live-" + token + "-" + i;
                    long firstSeen = nowMs - 1_000L;
                    bulk.upsert(dedupRow(token, fp, firstSeen,
                                    DedupExpiry.expiryMs(firstSeen, TTL_MS)))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    liveKeys.add(key(token, fp));
                }
            }
            for (int t = 0; t < STALE_TOKENS; t++) {
                long token = STALE_BASE_TOKEN + t;
                for (int i = 0; i < STALE_ROWS; i++) {
                    String fp = "stale-" + token + "-" + i;
                    long firstSeen = nowMs - TTL_MS - 5_000L; // expired well before now
                    bulk.upsert(dedupRow(token, fp, firstSeen,
                                    DedupExpiry.expiryMs(firstSeen, TTL_MS)))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    staleKeys.add(key(token, fp));
                }
            }
        } finally {
            bulk.flush();
        }
        long bulkMs = nanosToMs(System.nanoTime() - bulkStart);
        int bulkRows = liveKeys.size() + staleKeys.size();
        LOG.info("sig-perf-001: bulk fill — {} upserts via shared writer in {} ms ({} upserts/s)",
                bulkRows, bulkMs, ratePerSec(bulkRows, bulkMs));

        // ── Phase 1b: production store path — bounded putFirstSeen subset ──
        //    (validates the exact production write path against live Fluss
        //    and records its per-row durable latency as evidence).
        long storeStart = System.nanoTime();
        try (FlussFingerprintDedupStateStore store = openStore(tableName)) {
            for (int i = 0; i < STORE_PATH_ROWS; i++) {
                String fp = "store-path-" + i;
                long firstSeen = nowMs - 1_000L;
                store.putFirstSeen(STORE_PATH_TOKEN, VERSION, fp, firstSeen,
                        DedupExpiry.expiryMs(firstSeen, TTL_MS));
                liveKeys.add(key(STORE_PATH_TOKEN, fp));
            }
        }
        long storeMs = nanosToMs(System.nanoTime() - storeStart);
        LOG.info("sig-perf-001: store write path — {} putFirstSeen in {} ms (avg {} ms/row)",
                STORE_PATH_ROWS, storeMs, STORE_PATH_ROWS == 0 ? 0 : storeMs / STORE_PATH_ROWS);
        int written = liveKeys.size() + staleKeys.size(); // AFTER both write phases

        // ── Phase 2: sizing after write ───────────────────────────────────
        long afterWrite = scanCount(scratchTable, info);
        LOG.info("sig-perf-001: sizing after write — scan count {} == live {} + stale {}",
                afterWrite, liveKeys.size(), staleKeys.size());
        assertEquals(written, afterWrite,
                "steady-state entries = accepted rate × TTL horizon (one row per accepted fingerprint)");

        // ── Phase 3: cold-restart hydration (fresh store = cold cache) ────
        long hydrationTotalNs = 0;
        int liveFound = 0;
        LOG.info("sig-state-002: hydration — opening fresh store (cold-cache restart)");
        try (FlussFingerprintDedupStateStore store = openStore(tableName)) {
            LOG.info("sig-state-002: hydration — fresh store open, starting sampled lookups");
            // Sampled lookups: a cold restart must see every accepted
            // fingerprint as SEEN_LIVE from Fluss authority (SIG-STATE-002/003
            // — "cache says unseen" never accepts against the table).
            long lookupStart = System.nanoTime();
            int done = 0;
            for (int i = 0; i < liveKeys.size() && done < HYDRATION_SAMPLE; i += Math.max(1,
                    liveKeys.size() / HYDRATION_SAMPLE)) {
                String k = liveKeys.get(i);
                assertEquals(FingerprintDedupStateStore.Lookup.SEEN_LIVE,
                        store.lookup(tokenOf(k), VERSION, fpOf(k), nowMs),
                        "cold restart must rehydrate live fingerprint " + k);
                liveFound++;
                done++;
                if (done % 5 == 0) {
                    LOG.info("sig-state-002: hydration — {}/{} lookups done", done, HYDRATION_SAMPLE);
                }
            }
            hydrationTotalNs = System.nanoTime() - lookupStart;
            assertEquals(FingerprintDedupStateStore.Lookup.NOT_SEEN,
                    store.lookup(9_999_999L, VERSION, "never-written", nowMs),
                    "unseen fingerprint stays NOT_SEEN");

            // Bucket-read evidence (SIG-STATE-002: log-scan vs current-state
            // hydration read) — time the full-bucket scan the cleanup pass
            // drives, for the first stale token's bucket.
            long readStart = System.nanoTime();
            List<DedupExpiry.CleanupCandidate> bucketRead =
                    store.scanExpired(STALE_BASE_TOKEN, nowMs, Integer.MAX_VALUE);
            long readMs = nanosToMs(System.nanoTime() - readStart);
            LOG.info("sig-state-002: hydration read — {} sampled live lookups SEEN_LIVE in {} ms "
                            + "(avg {} ms/lookup); bucket read {} rows in {} ms",
                    liveFound, nanosToMs(hydrationTotalNs),
                    liveFound == 0 ? 0 : nanosToMs(hydrationTotalNs) / liveFound,
                    bucketRead.size(), readMs);
            assertTrue(bucketRead.size() >= STALE_ROWS,
                    "bucket read must surface every stale row (shared-bucket live rows excluded by expiry)");
        }

        // ── Phase 4: bounded re-entrant cleanup (delete/ack evidence) ─────
        long cleanupStart = System.nanoTime();
        int deleted = 0;
        int passes = 0;
        LOG.info("sig-state-001: cleanup — starting bounded scan+delete passes");
        try (FlussFingerprintDedupStateStore store = openStore(tableName)) {
            for (int t = 0; t < STALE_TOKENS; t++) {
                long token = STALE_BASE_TOKEN + t;
                List<DedupExpiry.CleanupCandidate> batch;
                while (!(batch = store.scanExpired(token, nowMs, CLEANUP_BATCH)).isEmpty()) {
                    store.delete(batch);
                    deleted += batch.size();
                    passes++;
                }
            }
        }
        long cleanupMs = nanosToMs(System.nanoTime() - cleanupStart);
        LOG.info("sig-state-001: cleanup — {} expired rows deleted in {} ms ({} rows/s) "
                        + "across {} bounded passes of {}",
                deleted, cleanupMs, ratePerSec(deleted, cleanupMs), passes, CLEANUP_BATCH);
        assertEquals(staleKeys.size(), deleted,
                "every expired row must be deleted by the bounded re-entrant pass");

        // Authoritative read path after delete: stale keys NOT_SEEN, live keys
        // still SEEN_LIVE (delete/ack semantics evidence — SIG-STATE-001).
        try (FlussFingerprintDedupStateStore store = openStore(tableName)) {
            for (int i = 0; i < staleKeys.size() && i < POST_DELETE_VERIFY; i += Math.max(1,
                    staleKeys.size() / POST_DELETE_VERIFY)) {
                String k = staleKeys.get(i);
                assertEquals(FingerprintDedupStateStore.Lookup.NOT_SEEN,
                        store.lookup(tokenOf(k), VERSION, fpOf(k), nowMs),
                        "deleted stale fingerprint " + k + " must be NOT_SEEN");
            }
            for (int i = 0; i < liveKeys.size() && i < POST_DELETE_VERIFY; i += Math.max(1,
                    liveKeys.size() / POST_DELETE_VERIFY)) {
                String k = liveKeys.get(i);
                assertEquals(FingerprintDedupStateStore.Lookup.SEEN_LIVE,
                        store.lookup(tokenOf(k), VERSION, fpOf(k), nowMs),
                        "live fingerprint " + k + " must survive cleanup");
            }
        }

        // Final plateau: only live rows remain (SIG-PERF-001 sizing row).
        long finalCount = scanCount(scratchTable, info);
        LOG.info("sig-perf-001: sizing after cleanup — scan count {} (live {}; stale removed)",
                finalCount, liveKeys.size());
        assertEquals(liveKeys.size(), finalCount,
                "cleanup must converge the table to the live set (size plateau = accepted rate × TTL)");

        LOG.info("sig-ext: PASS — write/hydration/cleanup externalization evidence on {}",
                tableName);
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static FlussFingerprintDedupStateStore openStore(String tableName) throws Exception {
        return FlussFingerprintDedupStateStore.open(bootstrap, "default", tableName, TIMEOUT);
    }

    /** Exact 6-column fingerprint_dedup schema mirroring DDL 24 v1. */
    private static Schema dedupSchema() {
        return Schema.newBuilder()
                .column("instrument_token", DataTypes.BIGINT())
                .column("fingerprint_version", DataTypes.STRING())
                .column("event_fingerprint", DataTypes.STRING())
                .column("first_seen_ms", DataTypes.BIGINT())
                .column("expiry_ms", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("instrument_token", "fingerprint_version", "event_fingerprint")
                .build();
    }

    /** Create a scratch fingerprint_dedup-shaped KV table; remember it for cleanup. */
    private static Table createScratchDedupTable(String name) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(dedupSchema())
                .distributedBy(16, "instrument_token")
                .property("table.kv.format-version", "2")
                .build();
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, td, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED_TABLES.add(name);
        return connection.getTable(path);
    }

    private static String key(long token, String fingerprint) {
        return token + "|" + VERSION + "|" + fingerprint;
    }

    /** 6-column dedup row in DDL order (the store's putFirstSeen layout). */
    private static GenericRow dedupRow(long token, String fingerprint,
            long firstSeenMs, long expiryMs) {
        return GenericRow.of(token,
                BinaryString.fromString(VERSION),
                BinaryString.fromString(fingerprint),
                firstSeenMs, expiryMs,
                BinaryString.fromString(FingerprintDedupTableColumns.SCHEMA_VERSION_V1));
    }

    private static long tokenOf(String key) {
        return Long.parseLong(key.split("\\|", 3)[0]);
    }

    private static String fpOf(String key) {
        return key.split("\\|", 3)[2];
    }

    /** Full-table row count (the production scanExpired read path). */
    private static long scanCount(Table table, TableInfo info) throws Exception {
        long count = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
            }
        }
        return count;
    }

    private static long nanosToMs(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static long ratePerSec(long count, long ms) {
        return ms <= 0 ? -1L : (count * 1_000L) / ms;
    }

    private static int intEnv(String key, int def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
    }

    private static long longEnv(String key, long def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
    }
}
