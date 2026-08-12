package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker 14 P6 (docs/08_implementation/14-candle-log-kv-replay-safety_2.md) —
 * end-to-end harness for the CANDLE-KV-REPLAY-001 replay contract.
 *
 * <p>The harness runs the <b>actual production graph</b>
 * ({@link SignalJob#buildTopology(SignalJobConfig)}: Fluss source → raw
 * validation → fingerprint dedup → 15s event-time window → LOG sink + KV
 * upsert sink → breakout detection → candidates KV sink) inside a Flink
 * {@link MiniClusterWithClientResource} against <b>scratch Fluss tables</b>
 * (unique per test: {@code p6_<epoch>_raw|_log|_kv|_cand}, created and
 * dropped by this test). Production tables and the live dev signaljob are
 * never touched.
 *
 * <p>Feed (deterministic, event-time ordered): tokens 1000/1001 × 23 windows
 * × 4 ticks (window {@code w}: prices open=10000+w, high=10050+w,
 * low=9950+w, close=10000+w), plus:
 * <ul>
 *   <li>window 4: one extra tick appended after the window's own ticks but
 *       before the window closes — must fold into the accumulator
 *       (close(w4)=7004, arrival-order semantics);</li>
 *   <li>window 5: one tick whose event time is inside w5 appended AFTER the
 *       watermark passed w5.end+lateness — must be dropped, candle
 *       uncorrected (close(w5)=10005, tick_count=4);</li>
 *   <li>window 17: token 1000 gets only 3 ticks — its window must still close
 *       on the shared watermark advanced by token 1001 (idle bucket does not
 *       block window close);</li>
 *   <li>window 22: breakout candle (open=11000, close=15000 &gt;
 *       max(high of previous 20) = 10071) — fires exactly one candidate per
 *       token ({@code breakout-20-bullish-trend-<token>-<windowEnd>});</li>
 *   <li>window 23: a pusher tick advances the watermark past w22's end so the
 *       final breakout window closes;</li>
 *   <li>5 invalid rows on token 2000 (schema version "3", price 0, qty −5,
 *       blank fingerprint, non-VALID validity) — all dropped by validation,
 *       no candle for token 2000.</li>
 * </ul>
 *
 * <p>Assertions:
 * <ul>
 *   <li><b>P6.1</b> — phase 1: LOG=46 rows / KV=46 distinct keys / 2
 *       candidates. Phase 2 (full replay from the same raw LOG, fresh state):
 *       LOG grows to 92 (append-only evidence trail re-emits), KV stays 46
 *       distinct keys with <i>identical business fields</i> (idempotent
 *       upsert), and candidates stay 2. Phase 3 (restore from the last
 *       completed checkpoint of phase 2 + 4 new ticks per token): LOG grows to
 *       exactly 96, including the two pending w23 pusher ticks held in
 *       checkpointed window state and two w24 candles; KV grows to 50. An
 *       offset-0 fallback would re-emit the original 46 windows and reach 142
 *       LOG rows. Existing KV business fields remain unchanged.</li>
 *   <li><b>P6.2</b> — {@link SignalJob#preflightTableContracts} fails closed
 *       on: KV missing, LOG missing, KV wrong kind (LOG as current), KV
 *       schema drift (20-col raw as current), unreachable coordinator; and
 *       passes for the scratch pair.</li>
 *   <li><b>P6.3</b> — invalid rows produce no candle; in-window late arrival
 *       folds in; beyond-lateness arrival is dropped and never corrects the
 *       emitted candle; per-token idle windows still close; emitted rows carry
 *       the canonical algorithm/configuration/schema versions.</li>
 * </ul>
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} —
 * skipped (with no MiniCluster startup) in the normal suite. Requires the dev
 * Fluss cluster ({@code FLUSS_BOOTSTRAP}, default {@code localhost:9123});
 * the test skips when the cluster is unreachable. Run:
 * {@code mvn -o test -Dtest=CandleGraphReplayIntegrationTest} with the gate
 * set.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P6: dual-sink graph replay + failure + data-quality")
class CandleGraphReplayIntegrationTest {


    private static final Logger LOG = LoggerFactory.getLogger(CandleGraphReplayIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /** 15000-aligned epoch anchor: floor(BASE/15000)*15000 == BASE (window alignment). */
    private static final long BASE = 1_699_999_995_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long TOKEN_A = 1000L;
    private static final long TOKEN_B = 1001L;
    private static final long TOKEN_BAD = 2000L;
    private static final int WINDOWS = 23; // w0..w22
    private static final String RULE_ID = "breakout-20-bullish-trend";

    private static final String CANONICAL_ALGORITHM = "candle-15s-v1";
    private static final String CANONICAL_CONFIGURATION = "1.0.0";

    private static final List<String> CREATED_TABLES = new ArrayList<>();

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    /**
     * MiniCluster resource. MiniClusterWithClientResource is a JUnit 4
     * ExternalResource (flink-runtime tests-jar), not a JUnit 5 extension —
     * its {@code before()}/{@code after()} are invoked explicitly from
     * {@link #connect()} / {@link #cleanup()}. {@code before()} registers the
     * TestStreamEnvironment that {@link SignalJob#buildTopology}'s
     * {@code getExecutionEnvironment(Configuration)} resolves to, so jobs
     * submit to this MiniCluster.
     */
    private static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeAll
    static void connect() throws Exception {
        MINI_CLUSTER.before();
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("p6: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("p6: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        for (String table : CREATED_TABLES) {
            try {
                admin.dropTable(TablePath.of("default", table), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("p6: dropped scratch table {}", table);
            } catch (Exception e) {
                LOG.warn("p6: drop {} failed: {}", table, e.getMessage());
            }
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
        MINI_CLUSTER.after();
    }

    // ── P6.2 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P6.2 preflight fails closed on missing/wrong-kind/drifted/unreachable")
    void preflightFailureInjection() throws Exception {
        ScratchSet s = createSet();

        // Happy path: the scratch pair satisfies the dual-write contract.
        SignalJob.preflightTableContracts(SignalJobConfig.from(envFor(s, null)));

        // KV table missing.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "CANDLE_CURRENT_TABLE",
                        "p6_" + s.suffix() + "_missing_kv"))),
                "missing KV current-state table must fail preflight");
        // LOG table missing.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "CANDLE_TABLE",
                        "p6_" + s.suffix() + "_missing_log"))),
                "missing LOG twin must fail preflight");
        // Wrong kind: the candle LOG (no PK) must never pass as the KV target.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "CANDLE_CURRENT_TABLE", s.logName()))),
                "LOG table as current-state target must fail preflight");
        // Schema drift: the 20-column raw LOG must never pass the 15-column check.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "CANDLE_CURRENT_TABLE", s.rawName()))),
                "schema-drifted target must fail preflight");
        // Unreachable coordinator — fail closed, never build a degraded graph.
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                SignalJobConfig.from(envFor(s, null, "FLUSS_BOOTSTRAP_SERVERS", "127.0.0.1:1"))),
                "unreachable Fluss cluster must fail preflight");
    }

    // ── P6.1 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P6.1 replay + restore: KV converges to one row per key with equal business "
            + "fields; LOG grows; restore resumes at offsets (no offset-0)")
    void dualSinkReplayAndRestoreIdempotency() throws Exception {
        ScratchSet s = createSet();
        appendFeed(s);

        // ── Phase 1: first pass ────────────────────────────────────────────
        JobClient job1 = startJob(envFor(s, null), "p6-phase1");
        awaitTrue(() -> safe(() -> kvCount(s) == 46), "phase-1 KV = 46 distinct keys", 180);
        Map<CandleKey, List<CandleRow>> log1 = readLogMap(s);
        Map<CandleKey, CandleRow> kv1 = readKvMap(s);
        assertFirstPass(s, log1, kv1);
        cancelAndFinish(job1, "phase1");
        LOG.info("p6: phase 1 done — LOG={} rows, KV={} keys, candidates={}",
                log1.size(), kv1.size(), candidateIds(s).size());

        // ── Phase 2: full replay (fresh state, same raw LOG) ───────────────
        JobClient job2 = startJob(envFor(s, null), "p6-phase2");
        awaitTrue(() -> safe(() -> logCount(s) == 92), "phase-2 LOG = 92 rows (46 re-emitted)", 180);
        Map<CandleKey, List<CandleRow>> log2 = readLogMap(s);
        Map<CandleKey, CandleRow> kv2 = readKvMap(s);
        assertReplayConverges(s, log1, log2, kv1, kv2);
        String restore = awaitStableCheckpoint(s, job2.getJobID(), 180);
        cancelAndFinish(job2, "phase2");
        LOG.info("p6: phase 2 done — LOG={} rows, KV={} keys, candidates={}, restore={}",
                log2.size(), kv2.size(), candidateIds(s).size(), restore);

        // The phase-2 checkpoint retains the valid pusher tick in w23. New w24
        // data closes that pending window, and the w25 pusher closes w24.
        appendWindow(s, 24, 10024L); // 4 ticks per token in w24 (flat, no signal)
        appendPusher(s, 25, 10025L); // advance the watermark past w24's end
        JobClient job3 = startJob(envFor(s, restore), "p6-phase3");
        awaitLogCount(s, 96, "phase-3 restore must emit pending w23 and new w24", 180);
        Map<CandleKey, CandleRow> kv3 = readKvMap(s);
        assertEquals(50, kv3.size(),
                "phase-3 KV must hold 46 old keys + pending w23 + new w24 for both tokens");
        assertExistingKeysAndBusinessFields(kv2, kv3,
                "restore must not change the business fields of already-written keys");
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            assertEquals(1, kv3.get(new CandleKey(token, BASE + 23 * WINDOW_MS)).tickCount(),
                    "restored pending pusher tick must close w23 exactly once");
            assertEquals(4, kv3.get(new CandleKey(token, BASE + 24 * WINDOW_MS)).tickCount(),
                    "post-restore w24 must contain its four ticks");
        }
        assertEquals(2, candidateIds(s).size(),
                "the pending and flat windows must not fire a new signal");
        cancelAndFinish(job3, "phase3");
        LOG.info("p6: phase 3 done — LOG={} rows, KV={} keys, candidates={}",
                logCount(s), kv3.size(), candidateIds(s).size());
    }


    // ── P6.3 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P6.3 data quality: invalid rows dropped, in-window late folds, "
            + "beyond-lateness dropped, idle window closes, canonical versions")
    void dataQualityRejectionsAndWindowSemantics() throws Exception {
        ScratchSet s = createSet();
        appendFeed(s);
        JobClient job = startJob(envFor(s, null), "p6-data-quality");
        awaitTrue(() -> safe(() -> kvCount(s) == 46), "KV = 46 distinct keys", 180);

        Map<CandleKey, List<CandleRow>> log = readLogMap(s);
        Map<CandleKey, CandleRow> kv = readKvMap(s);
        assertFirstPass(s, log, kv);

        // Invalid rows never become candles — token 2000 absent from LOG and KV.
        assertTrue(kv.keySet().stream().noneMatch(k -> k.token() == TOKEN_BAD),
                "invalid rows must not produce KV candles");
        assertTrue(log.keySet().stream().noneMatch(k -> k.token() == TOKEN_BAD),
                "invalid rows must not produce LOG candles");
        // LOG first pass: exactly one row per key (no duplicates yet).
        for (Map.Entry<CandleKey, List<CandleRow>> e : log.entrySet()) {
            assertEquals(1, e.getValue().size(), "first-pass LOG must hold one row per key: " + e.getKey());
        }
        // Emitted rows carry the canonical version triple (KV filter passes all).
        CandleRow sample = kv.get(new CandleKey(TOKEN_A, BASE + 10 * WINDOW_MS));
        assertEquals(CANONICAL_ALGORITHM, sample.algorithmVersion());
        assertEquals(CANONICAL_CONFIGURATION, sample.configurationVersion());
        assertEquals("2", sample.schemaVersion());

        cancelAndFinish(job, "data-quality");
        LOG.info("p6: data-quality done — LOG={} rows, KV={} keys, candidates={}",
                log.size(), kv.size(), candidateIds(s).size());
    }

    // ── feed ───────────────────────────────────────────────────────────────

    /** Appends the full deterministic feed (see class javadoc) to the scratch raw LOG. */
    private static void appendFeed(ScratchSet s) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            for (int w = 0; w < WINDOWS; w++) {
                for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                    int ticks = (w == 17 && token == TOKEN_A) ? 3 : 4;
                    appendWindowTicks(writer, token, w, ticks);
                }
                // In-window late arrival for window 4: appended after w4's own ticks
                // but before w5 (watermark still inside w4) — must fold in.
                if (w == 4) {
                    for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                        appendTick(writer, token, w, 14_900L, 7_000L + w, "L");
                    }
                }
            }
            // Pusher: closes window 22 (watermark = BASE+345000 = w22 end).
            appendPusher(writer, 23, 10_023L);
            // Beyond-lateness arrivals for window 5: event time inside w5, appended
            // after the watermark passed w5.end + allowedLateness — must be dropped.
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                appendTick(writer, token, 5, 14_900L, 8_000L + 5, "B");
            }
            appendInvalidRows(writer);
        } finally {
            writer.flush();
        }
    }

    /** Window w ticks at offsets [0, 5000, 10000, 14900]; w22 is the breakout window. */
    private static void appendWindowTicks(AppendWriter writer, long token, int w, int tickCount)
            throws Exception {
        if (w == 22) {
            appendTick(writer, token, w, 0L, 11_000L, "");
            appendTick(writer, token, w, 5_000L, 11_050L, "");
            appendTick(writer, token, w, 10_000L, 10_950L, "");
            appendTick(writer, token, w, 14_900L, 15_000L, "");
            return;
        }
        long base = 10_000L + w;
        appendTick(writer, token, w, 0L, base, "");
        appendTick(writer, token, w, 5_000L, base + 50L, "");
        appendTick(writer, token, w, 10_000L, base - 50L, "");
        if (tickCount > 3) {
            appendTick(writer, token, w, 14_900L, base, "");
        }
    }

    /** 4 in-order ticks in window 24 (flat candle — never fires a signal). */
    private static void appendWindow(ScratchSet s, int w, long base) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                appendTick(writer, token, w, 0L, base, "");
                appendTick(writer, token, w, 5_000L, base + 50L, "");
                appendTick(writer, token, w, 10_000L, base - 50L, "");
                appendTick(writer, token, w, 14_900L, base, "");
            }
        } finally {
            writer.flush();
        }
    }

    /** Advances the watermark past {@code w}'s end: event at w.start+5000, price base. */
    private static void appendPusher(AppendWriter writer, int w, long base) throws Exception {
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            appendTick(writer, token, w, 5_000L, base, "P");
        }
    }

    /** Standalone pusher (phase 3 feed) — opens its own writer. */
    private static void appendPusher(ScratchSet s, int w, long base) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            appendPusher(writer, w, base);
        } finally {
            writer.flush();
        }
    }

    private static void appendTick(AppendWriter writer, long token, int w, long offset, long price,
            String suffix) throws Exception {
        long eventTime = BASE + w * WINDOW_MS + offset;
        writer.append(toRawRow(token, eventTime, "fp-" + token + "-" + w + "-" + offset + suffix,
                "TRADE", price, 100L)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 5 invalid raw rows on token 2000 — every validation reason must drop them. */
    private static void appendInvalidRows(AppendWriter writer) throws Exception {
        writer.append(toRawRow(TOKEN_BAD, BASE + 1_000L, "fp-bad-schema", "TRADE", 10_000L, 100L,
                "VALID_TRADE", "3")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 2_000L, "fp-bad-price", "TRADE", 0L, 100L,
                "VALID_TRADE", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 3_000L, "fp-bad-qty", "TRADE", 10_000L, -5L,
                "VALID_TRADE", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 4_000L, "   ", "TRADE", 10_000L, 100L,
                "VALID_TRADE", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.append(toRawRow(TOKEN_BAD, BASE + 5_000L, "fp-bad-validity", "TRADE", 10_000L, 100L,
                "INVALID", "2")).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 20-column raw row mirroring DDL 02 / DdlBootstrap.RAW_TABLE_1_SCHEMA exactly. */
    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty) {
        return toRawRow(token, eventTime, fingerprint, tickType, price, qty, "VALID_TRADE", "2");
    }

    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty, String validity, String schemaVersion) {
        return GenericRow.of(
                bs(fingerprint), bs("v2"), bs("p6-conn"), 1L, token, bs("NSE"), bs("TEST"),
                eventTime, eventTime, eventTime, bs(tickType), price, qty, null,
                bs("h-" + fingerprint), bs("v2"), bs("1.0"), bs(validity), null, bs(schemaVersion));
    }

    // ── job control ────────────────────────────────────────────────────────

    /** Env map for the pinned SignalJobConfig; {@code recovery} null ⇒ explicit full replay. */
    private static Map<String, String> envFor(ScratchSet s, String recovery) {
        return envFor(s, recovery, null, null);
    }

    private static Map<String, String> envFor(ScratchSet s, String recovery,
            String overrideKey, String overrideValue) {
        Map<String, String> e = new HashMap<>();
        e.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        e.put("FLUSS_DATABASE", "default");
        e.put("RAW_TABLE", s.rawName());
        e.put("CANDLE_TABLE", s.logName());
        e.put("CANDLE_CURRENT_TABLE", s.kvName());
        e.put("SIGNAL_CANDIDATES_TABLE", s.candName());
        e.put("DEDUP_TTL_MS", "300000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("WATERMARK_OUT_OF_ORDER_MS", "5000");
        e.put("ALLOWED_LATENESS_MS", "5000");
        e.put("SOURCE_IDLE_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        e.put("RESTART_MAX_ATTEMPTS", "1");
        e.put("RESTART_DELAY_MS", "1000");
        e.put("DEPLOYMENT_ENV", "dev");
        e.put("STATE_BACKEND", "hashmap");
        e.put("CHECKPOINT_DIR", "file://" + s.checkpointDir().toAbsolutePath());
        e.put("PARALLELISM", "1");
        e.put("OTEL_COLLECTOR_HOST", "localhost:1");
        if (recovery == null) {
            e.put("ALLOW_FULL_REPLAY", "true");
        } else {
            e.put("STATE_RECOVERY_PATH", recovery);
        }
        if (overrideKey != null) {
            e.put(overrideKey, overrideValue);
        }
        return e;
    }

    /** Builds the production topology and submits it to the MiniCluster. */
    private static JobClient startJob(Map<String, String> env, String name) throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env);
        StreamExecutionEnvironment senv = SignalJob.buildTopology(config);
        JobClient client = senv.executeAsync(name);
        JobID jobId = client.getJobID();
        JobStatus status = awaitJobStatus(client, 90);
        if (status != JobStatus.RUNNING) {
            // Dump where the job is stuck from the MiniCluster execution graph.
            StringBuilder detail = new StringBuilder("job " + name + " stuck at " + status);
            try {
                AccessExecutionGraph graph =
                        MINI_CLUSTER.getMiniCluster().getExecutionGraph(jobId).get();
                for (AccessExecutionJobVertex v : graph.getVerticesTopologically()) {
                    detail.append("\n  vertex ").append(v.getName())
                            .append(" parallel=").append(v.getParallelism());
                    for (AccessExecutionVertex ev : v.getTaskVertices()) {
                        detail.append("\n    task ").append(ev.getParallelSubtaskIndex())
                                .append(" state=").append(ev.getExecutionState())
                                .append(" failure=")
                                .append(ev.getFailureInfo()
                                        .map(ErrorInfo::getExceptionAsString)
                                        .orElse("none"));
                    }
                }
            } catch (Exception e) {
                detail.append("\n  (graph dump failed: ").append(e).append(')');
            }
            assertTrue(false, detail.toString());
        }
        LOG.info("p6: job {} RUNNING", name);
        return client;
    }

    private static JobStatus awaitJobStatus(JobClient client, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            if (last == JobStatus.RUNNING || last == JobStatus.FINISHED || last == JobStatus.FAILED
                    || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(2000);
        }
        return last;
    }

    private static void cancelAndFinish(JobClient client, String phase) throws Exception {
        client.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        awaitTrue(() -> {
            try {
                return client.getJobStatus().get(5, TimeUnit.SECONDS) == JobStatus.CANCELED;
            } catch (Exception e) {
                return false;
            }
        }, "phase " + phase + " job CANCELED", 60);
    }

    // ── assertions ─────────────────────────────────────────────────────────

    /** P6.3 (first pass) + the phase-1 half of P6.1. */
    private static void assertFirstPass(ScratchSet s, Map<CandleKey, List<CandleRow>> log,
            Map<CandleKey, CandleRow> kv) throws Exception {
        assertEquals(46, kv.size(), "first pass: 23 windows x 2 tokens");
        assertEquals(46, log.size(), "first pass: LOG must hold one row per key");
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            // In-window late arrival folds in (arrival-order close semantics).
            assertEquals(7004, kv.get(new CandleKey(token, BASE + 4 * WINDOW_MS)).close(),
                    "in-window late tick must fold into the w4 candle");
            // Beyond-lateness arrival dropped, candle uncorrected.
            CandleRow w5 = kv.get(new CandleKey(token, BASE + 5 * WINDOW_MS));
            assertEquals(10005, w5.close(), "beyond-lateness tick must be dropped (close uncorrected)");
            assertEquals(4, w5.tickCount(), "beyond-lateness tick must not reach the accumulator");
            // Breakout window 22 fires.
            CandleRow w22 = kv.get(new CandleKey(token, BASE + 22 * WINDOW_MS));
            assertEquals(11000, w22.open());
            assertEquals(15000, w22.close());
        }
        // Token 1000's w17 has only 3 ticks — its window still closes on the
        // shared watermark advanced by token 1001 (idle bucket does not block).
        assertNotNull(kv.get(new CandleKey(TOKEN_A, BASE + 17 * WINDOW_MS)),
                "idle token's window must close on the shared watermark");
        // Exactly one candidate per token, deterministic id.
        Set<String> ids = candidateIds(s);
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            assertTrue(ids.contains(RULE_ID + "-" + token + "-" + (BASE + 23 * WINDOW_MS)),
                    "expected candidate " + RULE_ID + "-" + token + "-" + (BASE + 23 * WINDOW_MS)
                            + ", got " + ids);
        }
        assertEquals(2, ids.size(), "exactly one candidate per token");
    }

    /** P6.1 phase-2 replay convergence: LOG re-emits, KV stays one row per key, equal fields. */
    private static void assertReplayConverges(ScratchSet s, Map<CandleKey, List<CandleRow>> log1,
            Map<CandleKey, List<CandleRow>> log2, Map<CandleKey, CandleRow> kv1,
            Map<CandleKey, CandleRow> kv2) throws Exception {
        assertEquals(46, log1.size());
        assertEquals(46, kv1.size());
        // Map size is the distinct-key count; the LOG contract is its row count.
        int firstPassLogRows = log1.values().stream().mapToInt(List::size).sum();
        int replayLogRows = log2.values().stream().mapToInt(List::size).sum();
        assertEquals(firstPassLogRows * 2, replayLogRows,
                "full replay must re-emit every window exactly once");
        for (Map.Entry<CandleKey, List<CandleRow>> e : log1.entrySet()) {
            List<CandleRow> replayed = log2.get(e.getKey());
            assertNotNull(replayed, "replay must re-emit key " + e.getKey());
            assertEquals(2, replayed.size(), "replay must re-emit key exactly once more: " + e.getKey());
            assertBusinessFieldsEqual(e.getValue().get(0), replayed.get(1),
                    "replayed LOG row must carry the same business fields");
        }
        assertSameKeysAndBusinessFields(kv1, kv2, "replay must not change KV business fields");
        assertEquals(46, kv2.size(), "replay must not add KV keys");
        assertEquals(2, candidateIds(s).size(), "replay must not create extra candidates");
    }

    /** KV keys and all business fields (output_ts excluded) must be identical. */
    private static void assertSameKeysAndBusinessFields(Map<CandleKey, CandleRow> before,
            Map<CandleKey, CandleRow> after, String what) {
        assertEquals(before.keySet(), after.keySet(), what + " — key set must be unchanged");
        for (Map.Entry<CandleKey, CandleRow> e : before.entrySet()) {
            assertBusinessFieldsEqual(e.getValue(), after.get(e.getKey()),
                    what + " — key " + e.getKey());
        }
    }

    /** Existing KV rows survive a restore unchanged; new post-restore keys may be added. */
    private static void assertExistingKeysAndBusinessFields(Map<CandleKey, CandleRow> before,
            Map<CandleKey, CandleRow> after, String what) {
        assertTrue(after.keySet().containsAll(before.keySet()),
                what + " — restored KV must retain every existing key");
        for (Map.Entry<CandleKey, CandleRow> e : before.entrySet()) {
            assertBusinessFieldsEqual(e.getValue(), after.get(e.getKey()),
                    what + " — key " + e.getKey());
        }
    }

    private static void assertBusinessFieldsEqual(CandleRow a, CandleRow b, String what) {
        assertEquals(a.exchange(), b.exchange(), what);
        assertEquals(a.symbol(), b.symbol(), what);
        assertEquals(a.windowStart(), b.windowStart(), what);
        assertEquals(a.windowEnd(), b.windowEnd(), what);
        assertEquals(a.open(), b.open(), what);
        assertEquals(a.high(), b.high(), what);
        assertEquals(a.low(), b.low(), what);
        assertEquals(a.close(), b.close(), what);
        assertEquals(a.volume(), b.volume(), what);
        assertEquals(a.tickCount(), b.tickCount(), what);
        assertEquals(a.algorithmVersion(), b.algorithmVersion(), what);
        assertEquals(a.configurationVersion(), b.configurationVersion(), what);
        assertEquals(a.schemaVersion(), b.schemaVersion(), what);
    }

    // ── polling / reading ──────────────────────────────────────────────────

    private static void awaitTrue(BooleanSupplier cond, String what, long timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what);
    }

    private static void awaitLogCount(ScratchSet s, long expected, String what, long timeoutSeconds)
            throws Exception {
        long latest = -1;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            latest = logCount(s);
            if (latest == expected) {
                return;
            }
            if (latest > expected) {
                fail(what + ": expected " + expected + " LOG rows but observed " + latest);
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what
                + " (expected " + expected + " LOG rows, observed " + latest + ")");
    }

    private static boolean safe(ThrowingBooleanSupplier c) {
        try {
            return c.getAsBoolean();
        } catch (Exception e) {
            return false;
        }
    }

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    /** Waits until the LOG is stable for ≥3 polls AND a completed chk-N exists; returns it. */
    private static String awaitStableCheckpoint(ScratchSet s, JobID jobId, long timeoutSeconds)
            throws Exception {
        long last = -1;
        int stablePolls = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        String latest = null;
        while (System.nanoTime() < deadline) {
            long count = logCount(s);
            latest = latestCompletedCheckpoint(s, jobId);
            if (count == last) {
                stablePolls++;
            } else {
                stablePolls = 0;
            }
            last = count;
            if (stablePolls >= 3 && latest != null) {
                LOG.info("p6: LOG stable at {} rows for {} polls; restore target {}", count, stablePolls, latest);
                return latest;
            }
            Thread.sleep(5_000L);
        }
        String checkpointFailure = "none";
        try {
            AccessExecutionGraph graph = MINI_CLUSTER.getMiniCluster()
                    .getExecutionGraph(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            var checkpointStats = graph.getCheckpointStatsSnapshot();
            var counts = checkpointStats.getCounts();
            var failed = checkpointStats.getHistory().getLatestFailedCheckpoint();
            checkpointFailure = "enabled="
                    + graph.getCheckpointCoordinatorConfiguration().isCheckpointingEnabled()
                    + ", interval=" + graph.getCheckpointCoordinatorConfiguration().getCheckpointInterval()
                    + ", total=" + counts.getTotalNumberOfCheckpoints()
                    + ", completed=" + counts.getNumberOfCompletedCheckpoints()
                    + ", failed=" + counts.getNumberOfFailedCheckpoints()
                    + ", last=" + (failed == null ? "none" : failed.getFailureMessage());
        } catch (Exception e) {
            checkpointFailure = "unavailable: " + e;
        }
        fail("Timed out waiting for a stable checkpoint (LOG=" + last + ", latest chk=" + latest
                + ", latest failure=" + checkpointFailure + ")");
        return null; // unreachable
    }

    /** Newest completed checkpoint for {@code jobId}, as a {@code file://} URI. */
    private static String latestCompletedCheckpoint(ScratchSet s, JobID jobId) {
        Path dir = s.checkpointDir().resolve(jobId.toHexString());
        if (!Files.isDirectory(dir)) {
            return null;
        }
        long best = -1;
        try (Stream<Path> entries = Files.list(dir)) {
            for (Path p : (Iterable<Path>) entries::iterator) {
                String name = p.getFileName().toString();
                if (!name.startsWith("chk-") || !Files.isRegularFile(p.resolve("_metadata"))) {
                    continue;
                }
                try {
                    best = Math.max(best, Long.parseLong(name.substring("chk-".length())));
                } catch (NumberFormatException ignored) {
                    // not a checkpoint dir
                }
            }
        } catch (Exception e) {
            return null;
        }
        return best < 0 ? null : "file://" + dir.resolve("chk-" + best).toAbsolutePath();
    }

    private static long logCount(ScratchSet s) throws Exception {
        return scanAll(s.log(), s.logInfo()).size();
    }

    private static long kvCount(ScratchSet s) throws Exception {
        return scanAll(s.kv(), s.kvInfo()).size();
    }

    private static Map<CandleKey, List<CandleRow>> readLogMap(ScratchSet s) throws Exception {
        Map<CandleKey, List<CandleRow>> map = new HashMap<>();
        for (InternalRow r : scanAll(s.log(), s.logInfo())) {
            CandleKey k = new CandleKey(r.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                    r.getLong(CandleTableColumns.WINDOW_START));
            map.computeIfAbsent(k, ignored -> new ArrayList<>()).add(candleRow(r));
        }
        return map;
    }

    private static Map<CandleKey, CandleRow> readKvMap(ScratchSet s) throws Exception {
        Map<CandleKey, CandleRow> map = new HashMap<>();
        for (InternalRow r : scanAll(s.kv(), s.kvInfo())) {
            CandleKey k = new CandleKey(r.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                    r.getLong(CandleTableColumns.WINDOW_START));
            if (map.put(k, candleRow(r)) != null) {
                fail("KV table holds more than one row for " + k + " — upsert idempotency broken");
            }
        }
        return map;
    }

    private static Set<String> candidateIds(ScratchSet s) throws Exception {
        Set<String> ids = new HashSet<>();
        for (InternalRow r : scanAll(s.cand(), s.candInfo())) {
            ids.add(r.getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString());
        }
        return ids;
    }

    /** Scans every bucket of a table (works for LOG and KV — CountCandles probe precedent). */
    private static List<InternalRow> scanAll(Table table, TableInfo info) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    rows.add(it.next());
                }
            }
        }
        return rows;
    }

    private static CandleRow candleRow(InternalRow r) {
        return new CandleRow(
                r.getString(CandleTableColumns.EXCHANGE).toString(),
                r.getString(CandleTableColumns.SYMBOL).toString(),
                r.getLong(CandleTableColumns.WINDOW_START),
                r.getLong(CandleTableColumns.WINDOW_END),
                r.getLong(CandleTableColumns.OPEN_PAISE),
                r.getLong(CandleTableColumns.HIGH_PAISE),
                r.getLong(CandleTableColumns.LOW_PAISE),
                r.getLong(CandleTableColumns.CLOSE_PAISE),
                r.getLong(CandleTableColumns.VOLUME),
                r.getInt(CandleTableColumns.TICK_COUNT),
                r.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                r.getString(CandleTableColumns.CONFIGURATION_VERSION).toString(),
                r.getString(CandleTableColumns.SCHEMA_VERSION).toString(),
                r.getLong(CandleTableColumns.OUTPUT_TS));
    }

    // ── scratch tables ─────────────────────────────────────────────────────

    private static ScratchSet createSet() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        Path cpDir = Path.of(System.getProperty("java.io.tmpdir"), "p6-cp-" + suffix);
        String rawName = "p6_" + suffix + "_raw";
        String logName = "p6_" + suffix + "_log";
        String kvName = "p6_" + suffix + "_kv";
        String candName = "p6_" + suffix + "_cand";
        // One raw bucket gives this event-time test one source watermark; Fluss's
        // production 16-bucket routing is covered independently by bucket tests.
        Table raw = createTable(rawName, rawSchema(), null, 1, "raw LOG");
        Table log = createTable(logName, candleSchema(null), null, 16, "candle LOG");
        Table kv = createTable(kvName, candleSchema(List.of("instrument_token", "window_start")),
                List.of("instrument_token", "window_start"), 16, "candle KV");
        Table cand = createTable(candName, candidatesSchema(), List.of("candidate_id"), 16,
                "candidates KV");
        TableInfo rawInfo = tableInfo(rawName);
        TableInfo logInfo = tableInfo(logName);
        TableInfo kvInfo = tableInfo(kvName);
        TableInfo candInfo = tableInfo(candName);
        return new ScratchSet(suffix, rawName, logName, kvName, candName,
                raw, log, kv, cand, rawInfo, logInfo, kvInfo, candInfo, cpDir);
    }

    private static Table createTable(String name, Schema schema, List<String> pk, int bucketCount,
            String what) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                // LOG tables: instrument_token routing (mirrors raw_table_1 /
                // feature_candles_15s). KV tables: bucket key must be a subset of
                // the PK — candles use (instrument_token, window_start), candidates
                // use candidate_id (mirrors DDL 05).
                .distributedBy(bucketCount, pk == null ? "instrument_token" : pk.get(0))
                .build();
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, td, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED_TABLES.add(name);
        LOG.info("p6: created scratch {} table {}", what, name);
        return connection.getTable(path);
    }

    private static TableInfo tableInfo(String name) throws Exception {
        return admin.getTableInfo(TablePath.of("default", name))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 20-column raw LOG schema mirroring DDL 02 (DdlBootstrap.RAW_TABLE_1_SCHEMA). */
    private static Schema rawSchema() {
        return Schema.newBuilder()
                .column("event_fingerprint", DataTypes.STRING())
                .column("fingerprint_version", DataTypes.STRING())
                .column("connection_id", DataTypes.STRING())
                .column("connection_epoch", DataTypes.BIGINT())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("event_time", DataTypes.BIGINT())
                .column("ingest_ts", DataTypes.BIGINT())
                .column("ack_ts", DataTypes.BIGINT())
                .column("tick_type", DataTypes.STRING())
                .column("last_price_paise", DataTypes.BIGINT())
                .column("last_qty", DataTypes.BIGINT())
                .column("raw_payload", DataTypes.BYTES())
                .column("payload_hash", DataTypes.STRING())
                .column("decoder_version", DataTypes.STRING())
                .column("protocol_version", DataTypes.STRING())
                .column("validity_state", DataTypes.STRING())
                .column("validity_reason", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .build();
    }

    /** 15-column candle schema (both twins); PK set only for the KV twin. */
    private static Schema candleSchema(List<String> pk) {
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
        if (pk != null) {
            b.primaryKey(pk.toArray(new String[0]));
        }
        return b.build();
    }

    /** 22-column candidates KV schema mirroring DDL 05 (PK candidate_id). */
    private static Schema candidatesSchema() {
        return Schema.newBuilder()
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
                .column("schema_version", DataTypes.STRING())
                .primaryKey("candidate_id")
                .build();
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }

    /** One scratch table set — never shared between tests. */
    private record ScratchSet(
            String suffix,
            String rawName,
            String logName,
            String kvName,
            String candName,
            Table raw,
            Table log,
            Table kv,
            Table cand,
            TableInfo rawInfo,
            TableInfo logInfo,
            TableInfo kvInfo,
            TableInfo candInfo,
            Path checkpointDir) {}

    /** Candle row identity = (instrument_token, window_start). */
    private record CandleKey(long token, long windowStart) {}

    /** Business fields + output_ts of a candle row (output_ts excluded from equality checks). */
    private record CandleRow(
            String exchange,
            String symbol,
            long windowStart,
            long windowEnd,
            long open,
            long high,
            long low,
            long close,
            long volume,
            int tickCount,
            String algorithmVersion,
            String configurationVersion,
            String schemaVersion,
            long outputTs) {}
}
