package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DEC-038 SIG-STATE-002 (full job-restart rehydration flow) — the pending
 * "full job-restart half" of the row in {@code docs/08_implementation/04-signal-job.md}:
 * restart restores the compact checkpoint → verifies Fluss dedup-table
 * availability/compatibility → rehydrates the dedup working cache → a re-sent
 * fingerprint inside the TTL still dedupes. The store-side half is LANDED
 * ({@code FingerprintDedupExternalizationBenchmarkIT}); this test proves the
 * JOB-restart half against the LIVE dev Fluss cluster with the REAL production
 * store ({@link FlussFingerprintDedupStateStore}) and the REAL durable-write
 * path ({@link FingerprintDedupWriterFunction} → {@link FlussSink}) on a
 * scratch {@code fingerprint_dedup}-shaped table.
 *
 * <p><b>Phase 1 (job A):</b> the real dedup sub-graph — source → toRow →
 * keyBy(token) → {@link FingerprintDedupFunction} (store factory opens the
 * real {@link FlussFingerprintDedupStateStore} against the scratch table) →
 * main-output collecting sink + {@code fingerprint-dedup-write} side output →
 * {@link FingerprintDedupWriterFunction} (parallelism 1) → real
 * {@link FlussSink} upserting the scratch table (the exact SignalJob wiring).
 * N fingerprints are accepted; the run awaits all N emitted AND all N durably
 * visible in the live table (scan count) AND one completed checkpoint on top,
 * then cancels with the checkpoint retained. Cache cap is set BELOW N so
 * eviction runs — the restored checkpoint holds only the bounded cache.
 *
 * <p><b>Restart verification:</b> {@link SignalJob#preflightTableContracts}
 * must PASS against the scratch dedup table (the "verifies Fluss dedup-table
 * availability/compatibility" leg — {@code validateFingerprintDedupTable} is
 * ALWAYS-ON in the preflight).
 *
 * <p><b>Phase 2 (job B, strict restore):</b> a fresh MiniCluster restores the
 * latest phase-1 checkpoint at 2× parallelism with no
 * {@code allowNonRestoredState} and re-feeds all N fingerprints + 2 new ones.
 * Exactly the 2 NEW emit: the re-sent N dedupe through the rehydrated working
 * cache (cache hit) AND through Fluss authority on cache miss (query-on-miss —
 * a zero-state re-run would emit all N+2, and a storeless restart would
 * re-accept every evicted fingerprint).
 *
 * <p><b>Scratch-table discipline:</b> one {@code fingerprint_dedup}-shaped KV
 * table (plus 3 contract tables for the preflight leg) is created and dropped;
 * platform tables are never touched. Leader election needs a settle after
 * CREATE before the first durable write (evidence-gated, benchmark 2026-08-15).
 *
 * <p>Gate: {@code COMPUTE_INT_TEST_SIG_STATE_REHYDRATE=true} — skipped in the
 * normal suite. Live dev Fluss ({@code FLUSS_BOOTSTRAP}, default
 * {@code localhost:9123}); skips when unreachable. Host-runnable (embedded
 * MiniCluster + {@code file://} checkpoints + raw Fluss client). Run:
 * {@code COMPUTE_INT_TEST_SIG_STATE_REHYDRATE=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SigState002RehydrationRestoreIntegrationTest}
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_SIG_STATE_REHYDRATE", matches = "true")
@DisplayName("DEC-038 SIG-STATE-002: full job-restart rehydration — real Fluss store + sink, compact restore, re-sent dedupes (live)")
class SigState002RehydrationRestoreIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SigState002RehydrationRestoreIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(180);

    // Small smoke defaults (dev cluster acks ~100 ms/row): N rows accepted in
    // phase 1, cache cap below N so eviction forces the Fluss-authority path
    // on the phase-2 re-feed. Env-overridable per the repo's long-run gate.
    private static final int N =
            intEnv("SIG_STATE_REHYDRATE_ROWS", 200);
    private static final long CACHE_CAP =
            longEnv("SIG_STATE_REHYDRATE_CACHE_CAP", 100L);
    private static final long SETTLE_MS =
            longEnv("SIG_STATE_REHYDRATE_SETTLE_MS", 5_000L);

    /** Wall-clock base for event times (store expiry is judged by processing time). */
    private static final long WALL_T0 = System.currentTimeMillis();

    /** Collects main-output fingerprints (accepted first-seen), parallel-safe. */
    private static final List<String> EMITTED = Collections.synchronizedList(new ArrayList<>());

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;
    private static String suffix;
    private static String dedupTable;
    private static TableInfo dedupInfo;

    @BeforeAll
    static void connect() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("COMPUTE_INT_TEST_SIG_STATE_REHYDRATE", "false")),
                "Skipping — set COMPUTE_INT_TEST_SIG_STATE_REHYDRATE=true");
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        suffix = String.valueOf(System.nanoTime());
        try {
            org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("sig-002: connected to {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("sig-002: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
        dedupTable = "p6_" + suffix + "_dedup";
        Table scratch = ScratchTables.createDedup(connection, admin, dedupTable, TIMEOUT);
        // Freshly created buckets need leader-election settle before the first
        // durable write (NOT_LEADER_OR_FOLLOWER stall; evidence-gated).
        Thread.sleep(SETTLE_MS);
        LOG.info("sig-002: scratch dedup table {} created + {} ms settle", dedupTable, SETTLE_MS);
        dedupInfo = admin.getTableInfo(TablePath.of("default", dedupTable))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("sig-002: dedup table {} (id={}, buckets={}, PK={}, bucketKeys={})",
                dedupTable, dedupInfo.getTableId(), dedupInfo.getNumBuckets(),
                dedupInfo.getPrimaryKeys(), dedupInfo.getBucketKeys());
        // Non-dedup contract tables for the preflight leg (metadata-only).
        ScratchTables.create(connection, admin, "p6_" + suffix + "_cand",
                ScratchTables.candleSchema(), List.of("instrument_token", "window_start"),
                16, "candle KV", TIMEOUT);
        ScratchTables.create(connection, admin, "p6_" + suffix + "_sig",
                ScratchTables.signalLogSchema(), null, 16, "signal LOG", TIMEOUT);
        ScratchTables.create(connection, admin, "p6_" + suffix + "_cur",
                ScratchTables.signalCurrentSchema(), List.of("instrument_token"), 16,
                "signal current KV", TIMEOUT);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            ScratchTables.dropCreated(admin, TIMEOUT);
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /** Serializable row spec — GenericRowData is not serializable. */
    private record RowSpec(long token, long eventTime, String fingerprint)
            implements java.io.Serializable {}

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        env.put("FLUSS_DATABASE", "default");
        env.put("RAW_TABLE", "raw_table_1");
        env.put("CANDLE_TABLE", "p6_" + suffix + "_cand");
        env.put("SIGNAL_CANDIDATES_TABLE", "p6_" + suffix + "_sig");
        env.put("SIGNAL_CURRENT_TABLE", "p6_" + suffix + "_cur");
        env.put("DEDUP_STATE_TABLE", dedupTable);
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        // Bounded working cache below N — eviction runs, so the phase-2 re-feed
        // exercises BOTH the restored cache AND the Fluss-authority path.
        env.put("DEDUP_CACHE_MAX_ENTRIES", String.valueOf(CACHE_CAP));
        // Deterministic durable-write: writer flushes synchronously with each
        // emitted first-seen row (no 250 ms timer dependence at cancel time).
        env.put("DEDUP_WRITE_BATCH_SIZE", "1");
        return env;
    }

    private static List<RowSpec> rows(String prefix, int count, long token, long startEventTime) {
        List<RowSpec> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new RowSpec(token, startEventTime + i * 1_000L, prefix + "-" + i));
        }
        return out;
    }

    /** Emits rows once, then parks so the job stays RUNNING for checkpoints. */
    private static final class EmitOnceThenPark implements SourceFunction<RowSpec> {
        private static final long serialVersionUID = 1L;
        private final List<RowSpec> rows;
        private volatile boolean cancelled;

        EmitOnceThenPark(List<RowSpec> rows) {
            this.rows = rows;
        }

        @Override
        public void run(SourceContext<RowSpec> ctx) {
            for (RowSpec row : rows) {
                ctx.collectWithTimestamp(row, row.eventTime);
            }
            while (!cancelled) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    private static RowData toRow(RowSpec spec) {
        return TestRawRows.row(spec.token, spec.eventTime, spec.fingerprint, "TRADE", 100, 1);
    }

    /** Static collect sink (sink2 API) for the main output. */
    private static final class CollectingSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(RowData element, Context context) {
                    EMITTED.add(element.getString(RawTableColumns.EVENT_FINGERPRINT).toString());
                }

                @Override
                public void flush(boolean endOfInput) {
                    // results are collected in-process
                }

                @Override
                public void close() {
                    // nothing to release
                }
            };
        }
    }

    private static Configuration baseConfig(Path workDir) {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + workDir);
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        return config;
    }

    private static MiniClusterWithClientResource cluster(Configuration config, int parallelism) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberSlotsPerTaskManager(Math.max(4, parallelism))
                        .setNumberTaskManagers(1)
                        .build());
    }

    /**
     * The REAL dedup sub-graph (production wiring): source → toRow → keyBy(token)
     * → dedup (real {@link FlussFingerprintDedupStateStore} factory) → main sink;
     * first-seen rows → side output → writer (parallelism 1) → real
     * {@link FlussSink} upserting the scratch dedup table. Checkpoint storage +
     * restore go through the declarative Configuration, the same route
     * SignalJob uses; the SAME config is the MiniCluster's AND the env's.
     */
    private static JobClient submit(Configuration config,
            List<RowSpec> feed, boolean restore, String restorePath) throws Exception {
        if (restore) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restorePath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        SignalJobConfig jobConfig = SignalJobConfig.from(env());
        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<RowData> deduped =
                env.addSource(new EmitOnceThenPark(feed)).uid("src")
                        .map(SigState002RehydrationRestoreIntegrationTest::toRow).uid("map")
                        .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                        .process(new FingerprintDedupFunction(jobConfig, () ->
                                FlussFingerprintDedupStateStore.open(
                                        bootstrap, "default", dedupTable, TIMEOUT)))
                        .uid("dedup");
        deduped.sinkTo(new CollectingSink()).uid("out");
        // The durable-write path — exactly the SignalJob wiring (writer →
        // StallGuardedSink(FlussSink) → fingerprint_dedup).
        deduped.getSideOutput(FingerprintDedupFunction.DEDUP_WRITE_OUTPUT)
                .keyBy(row -> 0L)
                .process(new FingerprintDedupWriterFunction(jobConfig))
                .setParallelism(1)
                .uid("dedup-writer")
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(bootstrap)
                                .setDatabase("default")
                                .setTable(dedupTable)
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        jobConfig.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        jobConfig.sinkWriteStallTimeoutMs()))
                .uid("dedup-sink");
        return env.executeAsync();
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + JOB_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL.toMillis());
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
    }

    private static void awaitCompletedCheckpoints(JobClient job, Path cpRoot, int want)
            throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                fail("job " + status + " before " + want + " completed checkpoint(s) — see logs");
            }
            if (completedCheckpoints(cpRoot).size() >= want) {
                return;
            }
            Thread.sleep(1_000);
        }
        fail("timed out waiting for " + want + " completed checkpoint(s) under " + cpRoot);
    }

    private static void cancelAndWait(JobClient job) throws Exception {
        if (job.getJobStatus().get(10, TimeUnit.SECONDS) == JobStatus.RUNNING) {
            job.cancel().get(20, TimeUnit.SECONDS);
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (job.getJobStatus().get(5, TimeUnit.SECONDS) == JobStatus.CANCELED) {
                return;
            }
            Thread.sleep(500);
        }
        fail("phase job did not reach CANCELED");
    }

    private static List<Path> completedCheckpoints(Path cpRoot) throws java.io.IOException {
        List<Path> chks = new ArrayList<>();
        if (!Files.exists(cpRoot)) {
            return chks;
        }
        Files.walkFileTree(cpRoot, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir,
                    java.nio.file.attribute.BasicFileAttributes attrs) {
                String name = dir.getFileName().toString();
                if (name.startsWith("chk-") && Files.exists(dir.resolve("_metadata"))) {
                    chks.add(dir);
                    return java.nio.file.FileVisitResult.SKIP_SUBTREE;
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        chks.sort(java.util.Comparator.comparingLong(
                p -> Long.parseLong(p.getFileName().toString().substring(4))));
        return chks;
    }

    /** Latest completed checkpoint path (highest N) under the root, or null. */
    private static String latestCompletedCheckpoint(Path cpRoot) throws java.io.IOException {
        List<Path> chks = completedCheckpoints(cpRoot);
        return chks.isEmpty() ? null : chks.get(chks.size() - 1).toString();
    }

    /** Full-table row count through the production scan read path. */
    private static long scanCount() throws Exception {
        long count = 0;
        for (int b = 0; b < dedupInfo.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(dedupInfo.getTableId(), b);
            try (BatchScanner scanner = connection.getTable(TablePath.of("default", dedupTable))
                         .newScan().limit(Integer.MAX_VALUE).createBatchScanner(tb);
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

    /** scanCount without checked exceptions (for the await lambdas). */
    private static long safeScanCount() {
        try {
            return scanCount();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void deleteRecursively(Path root) throws java.io.IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (java.io.IOException e) {
                    // best-effort temp cleanup
                }
            });
        }
    }

    @Test
    @DisplayName("real store/sink + compact restore: re-sent fingerprints dedupe via Fluss authority (live)")
    void fullJobRestartRehydration() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.INFO);
        Path workDir = Files.createTempDirectory("sig-state-002-");
        try {
            // ── Restart verification leg: preflight passes the dedup table ──
            // (extended preflightTableContracts → validateFingerprintDedupTable,
            // ALWAYS-ON — the "verifies Fluss dedup-table availability/compatibility"
            // step of the DEC-038 restart contract).
            SignalJob.preflightTableContracts(SignalJobConfig.from(env()));

            // ── Phase 1 (job A): N accepted + durably in Fluss + checkpoint ──
            Path phase1 = workDir.resolve("jobA");
            Files.createDirectories(phase1);
            Configuration config1 = baseConfig(phase1);
            MiniClusterWithClientResource cluster1 = cluster(config1, 2);
            cluster1.before();
            try {
                EMITTED.clear();
                List<RowSpec> feed = rows("fp-a", N, 1L, WALL_T0);
                JobClient jobA = submit(config1, feed, false, null);
                awaitTrue("phase-1: all " + N + " first-seen emitted",
                        () -> EMITTED.size() == N);
                awaitTrue("phase-1: all " + N + " durably visible in the live dedup table",
                        () -> safeScanCount() == N);
                awaitCompletedCheckpoints(jobA, phase1, 1);
                long durable = scanCount();
                LOG.info("sig-002: phase-1 accepted={}, durable-table-rows={}, cache-cap={}",
                        EMITTED.size(), durable, CACHE_CAP);
                assertEquals(N, durable,
                        "every accepted first-seen must be durably upserted to the live table");
                cancelAndWait(jobA);
            } finally {
                cluster1.after();
            }

            String restore = latestCompletedCheckpoint(phase1);
            assertFalse(restore == null, "latest phase-1 checkpoint to restore from");

            // ── Phase 2 (job B): strict restore, re-sent dedupes ──────────
            Path phase2 = workDir.resolve("jobB-restore");
            Files.createDirectories(phase2);
            Configuration config2 = baseConfig(phase2);
            MiniClusterWithClientResource cluster2 = cluster(config2, 4); // 2x parallelism
            cluster2.before();
            try {
                EMITTED.clear();
                List<RowSpec> refeed = new ArrayList<>(rows("fp-a", N, 1L, WALL_T0));
                refeed.addAll(rows("fp-new", 2, 1L, WALL_T0 + 20_000_000L));
                long start = System.nanoTime();
                JobClient jobB = submit(config2, refeed, true, restore);
                awaitTrue("phase-2: restored job passes exactly the 2 NEW fingerprints",
                        () -> EMITTED.size() == 2);
                long restoreMs = (System.nanoTime() - start) / 1_000_000L;
                List<String> expected = rows("fp-new", 2, 1L, WALL_T0 + 20_000_000L).stream()
                        .map(RowSpec::fingerprint).sorted().collect(Collectors.toList());
                assertEquals(expected, EMITTED.stream().sorted().collect(Collectors.toList()),
                        "restored dedup state + Fluss authority must swallow all " + N
                                + " re-sent fingerprints (no full replay, no re-accept of evicted "
                                + "entries) and pass exactly the new ones");
                assertTrue(jobB.getJobStatus().get(15, TimeUnit.SECONDS) == JobStatus.RUNNING,
                        "restored job must run (strict restore did not fail)");
                // The durable set converged: still N rows from phase 1 (re-sent
                // fingerprints were NOT re-accepted as new first-seen) PLUS the
                // 2 genuinely new first-seen rows — re-sends do not re-upsert.
                awaitTrue("phase-2: table converged at N + 2 (" + (N + 2) + ")",
                        () -> safeScanCount() == N + 2L);
                LOG.info("sig-002: phase-2 restore-to-first-new-output={} ms (budget 30000)", restoreMs);
                assertTrue(restoreMs < 30_000L,
                        "restore must resume inside the 30 s budget, took " + restoreMs + " ms");
                cancelAndWait(jobB);
            } finally {
                cluster2.after();
            }
            LOG.info("sig-002: PASS — preflight OK, phase-1 durable={}, phase-2 exactly 2 new, "
                    + "table converged at {}", N, scanCount());
        } finally {
            deleteRecursively(workDir);
        }
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
