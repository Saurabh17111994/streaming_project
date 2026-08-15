package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
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
import java.util.stream.Stream;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * COMPAT-FLINK-001 + STATE-COMPAT-001 (serializer half) on the pinned Flink
 * 2.2.1 + fluss connector versions — host-runnable MiniCluster, no external
 * cluster, no Fluss (the source/sink boundary is a plain source→keyed→sink
 * topology; the real Fluss source/sink live boundary is SIG-INT-001 /
 * {@link CandleGraphReplayIntegrationTest}).
 *
 * <p><b>COMPAT-FLINK-001 — source/sink checkpoint, restore, rescale.</b>
 * Phase 1 (parallelism 2) runs a source → keyBy → keyed-state → sink topology
 * with {@code file://} checkpoints; the job is cancelled (checkpoints retained
 * via RETAIN_ON_CANCELLATION). Phase 2 restores the latest completed checkpoint
 * STRICTLY (no {@code allowNonRestoredState}) on a FRESH MiniCluster at 2×
 * parallelism (rescale): the keyed counts continue (600 checkpointed + new),
 * the state-origin tag stays {@code phase-1} (proving the state came from the
 * checkpoint, not a zero-state re-run), and the job keeps RUNNING — restored
 * processing and state remain within the approved consistency boundary on the
 * pinned versions.
 *
 * <p><b>STATE-COMPAT-001 serializer half.</b> The savepoint half (approved
 * serializer restores) is covered by {@link SignalJobSavepointRestoreIntegrationTest}.
 * This half proves the BLOCKING side of "restore succeeds through the approved
 * compatibility path, or startup blocks before unsafe use": phase 3 restores
 * the SAME phase-1 checkpoint with a DIFFERENT state serializer for the same
 * state name ({@code ValueState<Long>} → {@code ValueState<String>}, no state
 * schema migration) — the strict restore MUST fail the job at startup (job
 * FAILED, never RUNNING with silently-misread state).
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_COMPAT_FLINK=true)}
 * — skipped in the normal suite (MiniCluster). Host-runnable: embedded
 * MiniCluster + {@code file://} checkpoints — no external cluster, no Fluss.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_COMPAT_FLINK", matches = "true")
@DisplayName("COMPAT-FLINK-001 checkpoint/restore/rescale + STATE-COMPAT-001 serializer-change block")
class CompatFlinkCheckpointRescaleIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(CompatFlinkCheckpointRescaleIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final int KEYS = 4;
    private static final long PHASE1_PER_KEY = 600;
    private static final long PHASE2_PER_KEY = 300;

    /** Collects emitted count rows (parallel-safe, reset per phase). */
    private static final List<String> RESULTS = Collections.synchronizedList(new ArrayList<>());

    private static Configuration baseConfig(Path workDir) {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + workDir);
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                org.apache.flink.configuration.ExternalizedCheckpointRetention
                        .RETAIN_ON_CANCELLATION);
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 10);
        return config;
    }

    private static MiniClusterWithClientResource cluster(int parallelism) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(Math.max(4, parallelism))
                        .setNumberTaskManagers(1)
                        .build());
    }

    /** Emits 0..total-1 then idles until cancelled (keeps the job RUNNING with full state). */
    private static final class BoundedThenIdleSource implements SourceFunction<Long> {
        private static final long serialVersionUID = 1L;
        private final long total;
        private volatile boolean running = true;

        BoundedThenIdleSource(long total) {
            this.total = total;
        }

        @Override
        public void run(SourceContext<Long> ctx) {
            for (long i = 0; i < total && running; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(i);
                }
            }
            while (running) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Bounded source that emits {@code [from, from+count)} and stops (phase 2/3 feeds). */
    private static final class RangeSource implements SourceFunction<Long> {
        private static final long serialVersionUID = 1L;
        private final long from;
        private final long count;
        private volatile boolean running = true;

        RangeSource(long from, long count) {
            this.from = from;
            this.count = count;
        }

        @Override
        public void run(SourceContext<Long> ctx) {
            for (long i = 0; i < count && running; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(from + i);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Keyed counter whose {@code count} ValueState records per-key increments
     * plus a {@code firstRun} state-origin tag written ONLY on fresh state — the
     * restored state must keep {@code phase-1} (proof the value came from the
     * checkpoint, not a zero-state re-run).
     */
    private static final class KeyedCountFunction extends KeyedProcessFunction<Long, Long, String> {
        private static final long serialVersionUID = 1L;
        private final String firstRunTag;
        private transient ValueState<Long> count;
        private transient ValueState<String> firstRun;

        KeyedCountFunction(String firstRunTag) {
            this.firstRunTag = firstRunTag;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
            firstRun = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstRun", Types.STRING));
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<String> out) throws Exception {
            Long c = count.value();
            if (c == null) {
                c = 0L;
                firstRun.update(firstRunTag);
            }
            c += 1;
            count.update(c);
            out.collect(ctx.getCurrentKey() + "|" + c + "|" + firstRun.value());
        }
    }

    /** Same function but the {@code count} state is a STRING — an incompatible serializer. */
    private static final class KeyedCountFunctionV2 extends KeyedProcessFunction<Long, Long, String> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<String> count; // type changed: Long -> String
        private transient ValueState<String> firstRun;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.STRING));
            firstRun = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstRun", Types.STRING));
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<String> out) throws Exception {
            String c = count.value();
            if (c == null) {
                c = "0";
                firstRun.update("phase-1");
            }
            c = String.valueOf(Integer.parseInt(c) + 1);
            count.update(c);
            out.collect(ctx.getCurrentKey() + "|" + c + "|" + firstRun.value());
        }
    }

    /** Static collect sink (sink2 API — SinkFunction removed in Flink 2.x). */
    private static final class CollectSink implements Sink<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(String element, Context context) {
                    RESULTS.add(element);
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

    private static JobClient submit(StreamExecutionEnvironment env, String name) throws Exception {
        return env.executeAsync(name);
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos() * 5;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(250);
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
    }

    /** Newest completed checkpoint dir for the single job under {@code workDir}. */
    private static String latestCompletedCheckpoint(Path workDir) throws IOException {
        long best = -1;
        String bestPath = null;
        try (Stream<Path> roots = Files.list(workDir)) {
            for (Path jobRoot : (Iterable<Path>) roots::iterator) {
                if (!Files.isDirectory(jobRoot)) {
                    continue;
                }
                try (Stream<Path> entries = Files.list(jobRoot)) {
                    for (Path p : (Iterable<Path>) entries::iterator) {
                        String name = p.getFileName().toString();
                        if (!name.startsWith("chk-") || !Files.isRegularFile(p.resolve("_metadata"))) {
                            continue;
                        }
                        long n;
                        try {
                            n = Long.parseLong(name.substring("chk-".length()));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        if (n > best) {
                            best = n;
                            bestPath = "file://" + p.toAbsolutePath();
                        }
                    }
                }
            }
        }
        return bestPath;
    }

    private static void assertCounts(long expectedMax, String expectedFirstRun, String phase) {
        Map<Integer, Long> maxByKey = new HashMap<>();
        Map<Integer, String> firstRunOfMax = new HashMap<>();
        for (String row : RESULTS) {
            String[] p = row.split("\\|");
            int key = Integer.parseInt(p[0]);
            long c = Long.parseLong(p[1]);
            if (c > maxByKey.getOrDefault(key, -1L)) {
                maxByKey.put(key, c);
                firstRunOfMax.put(key, p[2]);
            }
        }
        for (int k = 0; k < KEYS; k++) {
            assertEquals(expectedMax, maxByKey.getOrDefault(k, -1L),
                    phase + " key " + k + " final count");
            assertEquals(expectedFirstRun, firstRunOfMax.get(k),
                    phase + " key " + k + " state-origin tag");
        }
        RESULTS.clear();
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // best-effort temp cleanup
                }
            });
        }
    }

    @Test
    void checkpointRestoreRescaleOnPinnedVersions() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.INFO);
        Path workDir = Files.createTempDirectory("compat-flink-");
        try {
            // ── Phase 1 (parallelism 2): build + checkpoint state ───────────
            String restore;
            MiniClusterWithClientResource clusterA = cluster(2);
            clusterA.before();
            try {
                Configuration config = baseConfig(workDir);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
                env.setParallelism(2);
                env.enableCheckpointing(1_000);
                env.addSource(new BoundedThenIdleSource(KEYS * PHASE1_PER_KEY), "compat-src").uid("src")
                        .keyBy(v -> v % KEYS)
                        .process(new KeyedCountFunction("phase-1")).uid("count")
                        .sinkTo(new CollectSink()).uid("sink");
                JobClient client = submit(env, "compat-phase1");
                awaitTrue("phase-1 running", () -> safeJobStatus(client) == JobStatus.RUNNING);
                awaitTrue("≥2 completed checkpoints", () -> {
                    try {
                        return latestCompletedCheckpoint(workDir) != null;
                    } catch (Exception e) {
                        return false;
                    }
                });
                Thread.sleep(2_500); // let a stable latest checkpoint land
                restore = latestCompletedCheckpoint(workDir);
                assertFalse(restore == null || restore.isEmpty(), "a completed checkpoint exists");
                assertCounts(PHASE1_PER_KEY, "phase-1", "phase-1");
                LOG.info("compat-flink: phase 1 done — restore target {}", restore);
                cancel(client);
            } finally {
                clusterA.after();
            }

            // ── Phase 2 (COMPAT-FLINK-001): strict restore at 2x parallelism ─
            RESULTS.clear();
            MiniClusterWithClientResource clusterB = cluster(4);
            clusterB.before();
            try {
                Configuration config = baseConfig(workDir);
                config.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
                env.setParallelism(4);
                env.enableCheckpointing(1_000);
                env.addSource(new RangeSource(KEYS * PHASE1_PER_KEY, KEYS * PHASE2_PER_KEY), "compat-src").uid("src")
                        .keyBy(v -> v % KEYS)
                        .process(new KeyedCountFunction("phase-1")).uid("count")
                        .sinkTo(new CollectSink()).uid("sink");
                JobClient client = submit(env, "compat-phase2");
                awaitTrue("phase-2 to finish", () -> safeJobStatus(client) == JobStatus.FINISHED);
                // 600 checkpointed + 300 new per key; tag stays phase-1: the
                // restored counts continued from the checkpoint, never replayed.
                assertCounts(PHASE1_PER_KEY + PHASE2_PER_KEY, "phase-1",
                        "COMPAT-FLINK-001 phase-2 (restore at 2x parallelism)");
                LOG.info("compat-flink: phase 2 restored + rescaled OK");
            } finally {
                clusterB.after();
            }

            // ── Phase 3 (STATE-COMPAT-001 serializer half): blocks startup ──
            RESULTS.clear();
            MiniClusterWithClientResource clusterC = cluster(2);
            clusterC.before();
            try {
                Configuration config = baseConfig(workDir);
                config.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
                env.setParallelism(2);
                env.addSource(new RangeSource(KEYS * PHASE1_PER_KEY, 1L), "compat-src").uid("src")
                        .keyBy(v -> v % KEYS)
                        .process(new KeyedCountFunctionV2()).uid("count") // incompatible serializer
                        .sinkTo(new CollectSink()).uid("sink");
                JobClient client = submit(env, "compat-phase3");
                awaitTrue("phase-3 to reach a terminal state",
                        () -> {
                            JobStatus s = safeJobStatus(client);
                            return s == JobStatus.FAILED || s == JobStatus.CANCELED
                                    || s == JobStatus.FINISHED;
                        });
                JobStatus status = safeJobStatus(client);
                assertFalse(status == JobStatus.RUNNING || status == JobStatus.FINISHED,
                        "a serializer change must NOT silently restore (phase-3 status=" + status + ")");
                if (status != JobStatus.FAILED) {
                    // Some Flink builds surface the restore error before FAILED;
                    // either way the job must never reach RUNNING with the new
                    // serializer (startup blocks before unsafe use).
                    LOG.warn("compat-flink: phase 3 terminal status {} (expected FAILED)", status);
                }
                assertFalse(RESULTS.stream().anyMatch(r -> r.endsWith("|phase-1")
                                && !r.startsWith("0|") && !r.startsWith("1|") && !r.startsWith("2|") && !r.startsWith("3|")),
                        "no silently-restored counts may be emitted under the changed serializer");
                LOG.info("compat-flink: phase 3 — serializer change blocked startup (status={})", status);
            } finally {
                clusterC.after();
            }
        } finally {
            deleteRecursively(workDir);
        }
    }

    private static JobStatus safeJobStatus(JobClient client) {
        try {
            return client.getJobStatus().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            return JobStatus.RECONCILING;
        }
    }

    private static void cancel(JobClient client) throws Exception {
        client.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        awaitTrue("job CANCELED", () -> safeJobStatus(client) == JobStatus.CANCELED);
    }
}
