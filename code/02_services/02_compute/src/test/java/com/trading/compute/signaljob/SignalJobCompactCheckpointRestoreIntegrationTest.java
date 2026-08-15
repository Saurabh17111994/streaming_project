package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * DEC-038 SIG-STATE-001 (compact-checkpoint restore, bounded size) — the last
 * pending half of the row in {@code docs/08_implementation/04-signal-job.md}:
 * the MiniCluster restore with the dedup table PRESENT, asserting (a) the
 * checkpoint does NOT grow with Fluss dedup cardinality (bounded working
 * state, never a second copy of the durable set) and (b) a strict restore
 * resumes from the compact checkpoint with no full replay, inside the 30 s
 * budget.
 *
 * <p><b>The dedup table is present.</b> The graph wires the real
 * {@link FingerprintDedupFunction} with the real writer path — first-seen rows
 * leave via the {@code fingerprint-dedup-write} side output, are batched by
 * {@link FingerprintDedupWriterFunction}, and land in a SHARED
 * {@link InMemoryFingerprintDedupStateStore} (the Fluss-table model) through a
 * store-put sink. The shared store survives across MiniCluster phases exactly
 * as the Fluss {@code fingerprint_dedup} table survives a job restart — so
 * restore re-feeds are decided by Fluss authority (cache miss → store
 * SEEN_LIVE), never re-accepted.
 *
 * <p><b>Bounded-checkpoint measurement.</b> Two jobs with the SAME cache cap
 * (500) but 5× the accepted fingerprints (2,000 vs 10,000 — the store grows
 * 2,000 → 12,000 via the shared table). The completed-checkpoint byte size is
 * walked on the local FS (the whole job root under retention 1 = one
 * checkpoint's state). DEC-038 hard invariant: the checkpoint is bounded by
 * the cache cap and does NOT grow with Fluss dedup cardinality — asserted as
 * {@code S(10k) < 3 × S(2k)}. This is also the regression guard for the
 * eviction timer-deletion fix: without it, evicted buckets leave orphaned
 * event-time timers, and the checkpoint grows ~with the accepted count.
 *
 * <p><b>No-full-replay restore.</b> Phase 3 restores the 10,000-fingerprint
 * job's latest checkpoint strictly (2× parallelism, no
 * {@code allowNonRestoredState}) on a fresh MiniCluster and re-feeds all
 * 10,000 + 2 new fingerprints: exactly the 2 NEW ones emit (a zero-state
 * re-run would emit all 10,002). Restore-submit → first-new-output duration is
 * recorded and must stay under the 30 s budget.
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_SIG_STATE_RESTORE=true)}
 * — skipped in the normal suite (MiniCluster). Host-runnable: embedded
 * MiniCluster + {@code file://} checkpoints — no external cluster, no Fluss,
 * no S3. Run:
 * {@code COMPUTE_INT_TEST_SIG_STATE_RESTORE=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SignalJobCompactCheckpointRestoreIntegrationTest}
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_SIG_STATE_RESTORE", matches = "true")
@DisplayName("DEC-038 SIG-STATE-001: checkpoint bounded across dedup cardinality; strict restore, no full replay, < 30s")
class SignalJobCompactCheckpointRestoreIntegrationTest {

    /** The Fluss-table model — shared across ALL phases (survives restarts). */
    private static final InMemoryFingerprintDedupStateStore SHARED_STORE =
            new InMemoryFingerprintDedupStateStore();

    /** Collects main-output fingerprints (accepted first-seen), parallel-safe. */
    private static final List<String> EMITTED = Collections.synchronizedList(new ArrayList<>());

    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final long CACHE_CAP = 500L;
    private static final int RUN_A_COUNT = 2_000;
    private static final int RUN_B_COUNT = 10_000;

    /**
     * Wall-clock base for event times. The store's read path judges expiry by
     * the PROCESSING-time clock (nowMs): a fixture with a historical event time
     * (e.g. 1_700_000_000_000) stores {@code first_seen} in the past, so every
     * row is {@code SEEN_EXPIRED} (re-acceptable) by restore time and the
     * restore re-feed re-accepts everything — the exact failure this test
     * caught. Basing event time on the real clock keeps every row live
     * ({@code SEEN_LIVE}) for the whole ~60 s run (TTL = 300 s).
     */
    private static final long WALL_T0 = System.currentTimeMillis();

    /** Serializable row spec — GenericRowData is not serializable. */
    private record RowSpec(long token, long eventTime, String fingerprint)
            implements java.io.Serializable {}

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        // Bounded working cache — the checkpoint must stay ≤ this, independent
        // of how many fingerprints the Fluss table holds.
        env.put("DEDUP_CACHE_MAX_ENTRIES", String.valueOf(CACHE_CAP));
        // Deterministic durable-write: the writer flushes synchronously with
        // each emitted first-seen row (batch size 1), so by the time the main
        // output has emitted all `count` rows, the store holds all `count` —
        // no dependence on the 250 ms processing-time flush timer vs the job
        // cancel. The writer's buffer is transient (never managed state), so
        // the checkpoint-size measurement is unaffected by this setting.
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

    /**
     * The fingerprint_dedup sink: durable putFirstSeen per first-seen row. In
     * production this is the FlussSink upserting {@code fingerprint_dedup};
     * here it writes the shared table model so the durable set survives the
     * job restart (Fluss authority).
     */
    private static final class StorePutSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(RowData row, Context context) {
                    try {
                        SHARED_STORE.putFirstSeen(
                                row.getLong(FingerprintDedupTableColumns.INSTRUMENT_TOKEN),
                                row.getString(FingerprintDedupTableColumns.FINGERPRINT_VERSION)
                                        .toString(),
                                row.getString(FingerprintDedupTableColumns.EVENT_FINGERPRINT)
                                        .toString(),
                                row.getLong(FingerprintDedupTableColumns.FIRST_SEEN_MS),
                                row.getLong(FingerprintDedupTableColumns.EXPIRY_MS));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void flush(boolean endOfInput) {
                    // in-memory store is already durable for the test's purpose
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
        // Retention 1: the job root at any moment holds ONE full checkpoint's
        // state (latest chk-N + its shared/taskowned files) — the whole-root
        // byte walk IS the latest checkpoint size.
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
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
     * The real dedup sub-graph: source → toRow → keyBy(token) → dedup → main
     * output sink; first-seen rows → side output → writer → store-put sink.
     * Checkpoint storage + restore go through the declarative Configuration
     * (the same route SignalJob uses — Flink 2.2.1 removed
     * {@code CheckpointConfig.setCheckpointStorage}; restore reads
     * {@code StateRecoveryOptions.SAVEPOINT_PATH}). The SAME configuration is
     * the MiniCluster's (JobManager resolves checkpoint storage from its own
     * config) AND the env's — the FS walk must find the checkpoints under the
     * phase's checkpoint root.
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

        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<RowData> deduped =
                env.addSource(new EmitOnceThenPark(feed)).uid("src")
                        .map(SignalJobCompactCheckpointRestoreIntegrationTest::toRow).uid("map")
                        .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                        .process(new FingerprintDedupFunction(
                                SignalJobConfig.from(env()), () -> SHARED_STORE))
                        .uid("dedup");
        deduped.sinkTo(new CollectingSink()).uid("out");
        // The durable-write path: side output → keyBy(constant) → batched
        // writer (parallelism 1) → store sink — the exact SignalJob wiring.
        deduped.getSideOutput(FingerprintDedupFunction.DEDUP_WRITE_OUTPUT)
                .keyBy(row -> 0L)
                .process(new FingerprintDedupWriterFunction(SignalJobConfig.from(env())))
                .setParallelism(1)
                .uid("dedup-writer")
                .sinkTo(new StorePutSink())
                .uid("dedup-sink");
        return env.executeAsync();
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL.toMillis());
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
    }

    /** Completed chk-N dirs under the checkpoint root (any depth), sorted by N. */
    private static List<Path> completedCheckpoints(Path cpRoot) throws IOException {
        List<Path> chks = new ArrayList<>();
        if (!Files.exists(cpRoot)) {
            return chks;
        }
        java.nio.file.Files.walkFileTree(cpRoot, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir,
                    BasicFileAttributes attrs) {
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

    /**
     * Latest completed checkpoint path (highest N), or null. Retention 1 means
     * it is also the only chk-N present; the job root holds its shared +
     * taskowned state.
     */
    private static String latestCompletedCheckpoint(Path cpRoot) throws IOException {
        List<Path> chks = completedCheckpoints(cpRoot);
        return chks.isEmpty() ? null : chks.get(chks.size() - 1).toString();
    }

    /** Total bytes under a path (recursive). */
    private static long bytesUnder(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0L;
        }
        final long[] total = {0L};
        java.nio.file.Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                total[0] += attrs.size();
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        return total[0];
    }

    /** The job root holding the latest checkpoint (parent chain up to cpRoot). */
    private static Path checkpointJobRoot(Path cpRoot, Path chk) {
        Path p = chk.getParent();
        while (p != null && !p.equals(cpRoot) && !p.getParent().equals(cpRoot)) {
            p = p.getParent();
        }
        return p;
    }

    /**
     * Latest checkpoint's full state size: the whole job root (latest chk-N +
     * its shared/taskowned siblings). Retention 1 + RETAIN_ON_CANCELLATION
     * keeps exactly this set on disk.
     */
    private static long latestCheckpointBytes(Path cpRoot) throws IOException {
        String latest = latestCompletedCheckpoint(cpRoot);
        assertFalse(latest == null, "no completed checkpoint under " + cpRoot);
        return bytesUnder(checkpointJobRoot(cpRoot, Path.of(latest)));
    }

    private static void awaitCompletedCheckpoints(JobClient job, Path cpRoot, int want)
            throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                fail("job " + status + " before " + want + " completed checkpoint(s) — "
                        + "see logs; store rows=" + SHARED_STORE.rowCount());
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

    @Test
    @DisplayName("checkpoint bounded across 5x dedup cardinality; strict restore, no full replay, < 30s")
    void compactCheckpointBoundedAndRestoresWithoutFullReplay() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.INFO);
        Path workDir = Files.createTempDirectory("sig-state-001-");
        try {
            // ---- Job A: 2,000 fingerprints, cache cap 500 -------------------
            long sA = runSizedJob(workDir, "jobA", "fp-a", RUN_A_COUNT, RUN_A_COUNT);
            // ---- Job B: 10,000 fingerprints (5x), SAME cache cap ------------
            long sB = runSizedJob(workDir, "jobB", "fp-b", RUN_B_COUNT,
                    (long) RUN_A_COUNT + RUN_B_COUNT);

            long storeRows = SHARED_STORE.rowCount();
            System.out.println("SIG-STATE-001[checkpoint-size] S(2k)=" + sA
                    + " bytes, S(10k)=" + sB + " bytes, ratio=" + (sB / (double) sA)
                    + ", store-rows=" + storeRows + ", cache-cap=" + CACHE_CAP);
            // Measured: fixed = ratio ~1.00 (54465 vs 54482); broken (no timer
            // deletion on eviction) = ratio ~2.70 (79982 vs 215982 — the
            // orphaned event-time timers accumulate in the checkpoint). 1.5x
            // sits well between, so the assertion guards the DEC-038 invariant
            // with real margin while never flaking on FS overhead.
            assertTrue(sB < 3L * sA / 2L,
                    "checkpoint must NOT grow with Fluss dedup cardinality: 5x fingerprints "
                            + "(2,000 -> 10,000, store " + storeRows + ") but checkpoint "
                            + sB + " < 1.5x " + sA + "=" + (3L * sA / 2L) + " — the checkpoint "
                            + "duplicates the durable dedup set (bounded-cache/timer invariant "
                            + "broken, DEC-038 hard rule)");
            assertEquals(12_000, storeRows,
                    "the shared dedup table holds every first-seen fingerprint (2,000 + 10,000)");

            // ---- Phase 3: strict restore of the 10,000-fingerprint job -----
            // Scope to JOB B's root: both jobA and jobB have a chk-1, so a
            // whole-tree scan ties on the checkpoint number and could restore
            // the WRONG (2,000-fingerprint) state.
            String restore = latestCompletedCheckpoint(workDir.resolve("jobB"));
            assertFalse(restore == null, "latest jobB checkpoint to restore from");
            long restoreStart = System.nanoTime();
            runRestoreAndVerify(workDir, restore);
            long restoreMs = (System.nanoTime() - restoreStart) / 1_000_000L;
            System.out.println("SIG-STATE-001[restore] restore-to-first-new-output="
                    + restoreMs + " ms (budget 30000)");
            assertTrue(restoreMs < 30_000L,
                    "restore must resume inside the 30 s budget, took " + restoreMs + " ms");
        } finally {
            deleteRecursively(workDir);
        }
    }

    /**
     * Run one sized job (fresh MiniCluster): await all `count` first-seen
     * emitted + durable in the shared store + one completed checkpoint ON TOP
     * of that state (retention 1 keeps exactly the latest chk-N on disk), then
     * return the checkpoint's byte size. The main-output emit and the
     * side-output → writer → store write cross a network shuffle (writer is
     * parallelism 1), so the store count is awaited too — no in-flight rows at
     * measure time. `expectedStoreRows` is the CUMULATIVE table size after this
     * run (2000 after run A, 12000 after run B).
     */
    private long runSizedJob(Path workDir, String runId, String fpPrefix, int count,
            long expectedStoreRows) throws Exception {
        Path sub = workDir.resolve(runId);
        Files.createDirectories(sub);
        Configuration config = baseConfig(sub);
        MiniClusterWithClientResource cluster = cluster(config, 2);
        cluster.before();
        try {
            EMITTED.clear();
            JobClient job = submit(config, rows(fpPrefix, count, 1L, WALL_T0), false, null);
            // State fully built FIRST (main output + durable store write), then
            // one completed checkpoint ON TOP of it — the measured bytes are
            // the checkpoint that carries all `count` fingerprints' bounded
            // working state.
            awaitTrue(fpPrefix + ": all " + count + " first-seen emitted",
                    () -> EMITTED.size() == count);
            awaitTrue(fpPrefix + ": all " + count + " durable in the store",
                    () -> SHARED_STORE.rowCount() >= expectedStoreRows);
            awaitCompletedCheckpoints(job, sub, 1);
            long bytes = latestCheckpointBytes(sub);
            cancelAndWait(job);
            return bytes;
        } finally {
            cluster.after();
        }
    }

    /** Fresh MiniCluster, strict restore, re-feed all 10,000 + 2 new. */
    private void runRestoreAndVerify(Path workDir, String restorePath) throws Exception {
        EMITTED.clear();
        Path sub = workDir.resolve("jobB-restore");
        Files.createDirectories(sub);
        Configuration config = baseConfig(sub);
        MiniClusterWithClientResource cluster = cluster(config, 4); // 2x parallelism
        cluster.before();
        try {
            List<RowSpec> refeed = new ArrayList<>(rows("fp-b", 10_000, 1L, WALL_T0));
            refeed.addAll(rows("fp-new", 2, 1L, WALL_T0 + 20_000_000L));
            JobClient job = submit(config, refeed, true, restorePath);
            awaitTrue("restored job to pass exactly the 2 NEW fingerprints",
                    () -> EMITTED.size() == 2);
            List<String> emitted = new ArrayList<>(EMITTED);
            List<String> expected = rows("fp-new", 2, 1L, WALL_T0 + 20_000_000L).stream()
                    .map(RowSpec::fingerprint).sorted().collect(Collectors.toList());
            assertEquals(expected, emitted.stream().sorted().collect(Collectors.toList()),
                    "restored dedup state + Fluss authority must swallow all 10,000 re-fed "
                            + "fingerprints (no full replay) and pass exactly the new ones — a "
                            + "zero-state re-run would have emitted all 10,002");
            assertTrue(job.getJobStatus().get(15, TimeUnit.SECONDS) == JobStatus.RUNNING,
                    "restored job must run (strict restore did not fail)");
            cancelAndWait(job);
        } finally {
            cluster.after();
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // best-effort temp cleanup
                }
            });
        }
    }
}
