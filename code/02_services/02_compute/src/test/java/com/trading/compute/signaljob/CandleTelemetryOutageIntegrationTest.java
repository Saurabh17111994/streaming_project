package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker 14 P8.2 box 5/6 — telemetry outage is non-blocking, on the REAL
 * {@link SignalJob} graph with the {@link ComputeOtlpEmitter} actually STARTED
 * (the one path every prior graph test skipped: {@code buildTopology} alone
 * never starts the emitter; {@code SignalJob.run} does).
 *
 * <p>The emitter is pointed at {@code 127.0.0.1:1} (refused port — the collector
 * is fully absent), mirroring {@code SignalJob.run}: {@code recordStartupMode}
 * then {@code start()} before {@code executeAsync}. The graph must close every
 * candle window of the canonical 46-row feed, the LOG and KV must both reach 46
 * rows (dual-write unaffected), the scheduler flush thread must stay alive, and
 * a synchronous {@code flushOnce()} must surface the transport failure as an
 * {@link IOException} while {@code flush()} swallows it — telemetry off the
 * critical path.
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} — same
 * dev-cluster gate as P6/P4.3. Scratch tables only; no live-cluster disturbance.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P8.2: telemetry outage does not block the SignalJob graph")
class CandleTelemetryOutageIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(CandleTelemetryOutageIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final long BASE = 1_699_999_995_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long TOKEN_A = 1000L;
    private static final long TOKEN_B = 1001L;
    private static final int WINDOWS = 23; // w0..w22

    private static final List<String> CREATED_TABLES = new ArrayList<>();
    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("p82-outage: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("p82-outage: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        for (String table : CREATED_TABLES) {
            try {
                admin.dropTable(TablePath.of("default", table), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                LOG.warn("p82-outage: drop {} failed: {}", table, e.getMessage());
            }
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("collector outage (dead port): candles still close, flush thread alive, IO surfaced")
    void collectorOutageDoesNotBlockSignalJobGraph() throws Exception {
        ScratchSet s = createSet();
        MiniClusterWithClientResource cluster = newMiniCluster();
        JobClient job = null;
        ComputeOtlpEmitter otlp = null;
        cluster.before();
        try {
            // Mirror SignalJob.run's telemetry wiring exactly — but the collector
            // endpoint is a refused port: the collector is ABSENT.
            ComputeOtlpEmitter.recordStartupMode(0); // RESTORE-mode gauge, like run()
            otlp = new ComputeOtlpEmitter("127.0.0.1:1");
            otlp.start();

            // Job startup must not block on telemetry: the emitter is started
            // BEFORE the graph, exactly as in run(), and the job still reaches
            // RUNNING.
            appendFeed(s);
            job = startJob(baseEnv(s), "p82-outage");
            assertTrue(flushThreadAlive(), "emitter scheduler thread must be alive");

            // The transport failure is real and surfaced at the flushOnce() seam:
            // the collector is unreachable, so the exporter cannot deliver. The
            // scheduled flush() swallows it (non-blocking); flushOnce() reports it.
            try {
                otlp.flushOnce();
                fail("flushOnce() against a refused port must throw IOException");
            } catch (IOException expected) {
                LOG.info("p82-outage: flushOnce() surfaced transport failure: {}",
                        expected.getMessage());
            }

            // Telemetry must not stop SignalJob processing: the canonical feed
            // closes every window on the LOG and the KV despite the outage.
            awaitLogCount(s, WINDOWS * 2L,
                    "candles close with the collector down (46 rows)", 180);
            long kv = kvCount(s);
            assertEquals(WINDOWS * 2L, kv,
                    "KV dual-write must be complete despite the telemetry outage");
            assertTrue(flushThreadAlive(),
                    "emitter scheduler thread must survive the outage (swallow, no rethrow)");
            LOG.info("p82-outage: LOG=46 KV={} with collector absent — processing unaffected",
                    kv);
        } finally {
            if (otlp != null) {
                otlp.close();
            }
            try {
                if (job != null) {
                    job.cancel().get(5, TimeUnit.SECONDS);
                }
            } catch (Exception ignored) {
                // best-effort: unblocks MiniCluster teardown
            }
            cluster.after();
        }
    }

    private static boolean flushThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> "compute-otlp-flush".equals(t.getName()) && t.isAlive());
    }

    // ── harness (same shape as P6.2/P6.3) ─────────────────────────────────

    private static MiniClusterWithClientResource newMiniCluster() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    private static Map<String, String> baseEnv(ScratchSet s) {
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
        e.put("RESTART_MAX_ATTEMPTS", "0");
        e.put("RESTART_DELAY_MS", "1000");
        e.put("DEPLOYMENT_ENV", "dev");
        e.put("STATE_BACKEND", "hashmap");
        e.put("CHECKPOINT_DIR", "file://" + s.checkpointDir().toAbsolutePath());
        e.put("PARALLELISM", "1");
        e.put("OTEL_COLLECTOR_HOST", "127.0.0.1:1"); // the outage: collector absent
        e.put("ALLOW_FULL_REPLAY", "true");
        return e;
    }

    private static JobClient startJob(Map<String, String> env, String name) throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env);
        StreamExecutionEnvironment senv = SignalJob.buildTopology(config);
        JobClient client = senv.executeAsync(name);
        JobStatus status = awaitJobStatus(client, 90);
        assertEquals(JobStatus.RUNNING, status, "job " + name + " must reach RUNNING, got " + status);
        LOG.info("p82-outage: job {} RUNNING", name);
        return client;
    }

    private static JobStatus awaitJobStatus(JobClient client, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.sleep(300);
                continue;
            }
            if (last == JobStatus.RUNNING || last == JobStatus.FINISHED || last == JobStatus.FAILED
                    || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(2_000L);
        }
        return last;
    }

    /** 46-row canonical feed (same as P6/P4.3): 23 windows x 2 tokens + pusher. */
    private static void appendFeed(ScratchSet s) throws Exception {
        AppendWriter writer = s.raw().newAppend().createWriter();
        try {
            for (int w = 0; w < WINDOWS; w++) {
                for (long token : new long[] {TOKEN_A, TOKEN_B}) {
                    appendWindowTicks(writer, token, w);
                }
            }
            appendPusher(writer, WINDOWS, 10_023L);
        } finally {
            writer.flush();
        }
    }

    private static void appendWindowTicks(AppendWriter writer, long token, int w) throws Exception {
        long base = 10_000L + w;
        appendTick(writer, token, w, 0L, base);
        appendTick(writer, token, w, 5_000L, base + 50L);
        appendTick(writer, token, w, 10_000L, base - 50L);
        appendTick(writer, token, w, 14_900L, base);
    }

    private static void appendPusher(AppendWriter writer, int w, long base) throws Exception {
        for (long token : new long[] {TOKEN_A, TOKEN_B}) {
            appendTick(writer, token, w, 5_000L, base);
        }
    }

    private static void appendTick(AppendWriter writer, long token, int w, long offset, long price)
            throws Exception {
        long eventTime = BASE + w * WINDOW_MS + offset;
        writer.append(toRawRow(token, eventTime, "fp-" + token + "-" + w + "-" + offset,
                "TRADE", price, 100L)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static GenericRow toRawRow(long token, long eventTime, String fingerprint,
            String tickType, long price, long qty) {
        return GenericRow.of(
                bs(fingerprint), bs("v2"), bs("p82-conn"), 1L, token, bs("NSE"), bs("TEST"),
                eventTime, eventTime, eventTime, bs(tickType), price, qty, null,
                bs("h-" + fingerprint), bs("v2"), bs("1.0"), bs("VALID_TRADE"), null, bs("2"));
    }

    // ── reads ─────────────────────────────────────────────────────────────

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

    private static long logCount(ScratchSet s) throws Exception {
        return scanAll(s.log(), s.logInfo()).size();
    }

    private static long kvCount(ScratchSet s) throws Exception {
        return scanAll(s.kv(), s.kvInfo()).size();
    }

    /** Scans every bucket of a table (LOG and KV both work — P6 precedent). */
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

    // ── scratch tables ─────────────────────────────────────────────────────

    private static ScratchSet createSet() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        Path cpDir = Path.of(System.getProperty("java.io.tmpdir"), "p82-cp-" + suffix);
        String rawName = "p82_" + suffix + "_raw";
        String logName = "p82_" + suffix + "_log";
        String kvName = "p82_" + suffix + "_kv";
        String candName = "p82_" + suffix + "_cand";
        Table raw = createTable(rawName, rawSchema(), null, 1, "raw LOG");
        Table log = createTable(logName, candleSchema(null), null, 16, "candle LOG");
        Table kv = createTable(kvName, candleSchema(List.of("instrument_token", "window_start")),
                List.of("instrument_token", "window_start"), 16, "candle KV");
        Table cand = createTable(candName, candidatesSchema(), List.of("candidate_id"), 16,
                "candidates KV");
        return new ScratchSet(rawName, logName, kvName, candName, raw, log, kv, cand,
                tableInfo(rawName), tableInfo(logName), tableInfo(kvName), tableInfo(candName),
                cpDir);
    }

    private static Table createTable(String name, Schema schema, List<String> pk, int bucketCount,
            String what) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(bucketCount, pk == null ? "instrument_token" : pk.get(0))
                .build();
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, td, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED_TABLES.add(name);
        LOG.info("p82-outage: created scratch {} table {}", what, name);
        return connection.getTable(path);
    }

    private static TableInfo tableInfo(String name) throws Exception {
        return admin.getTableInfo(TablePath.of("default", name))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

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

    private record ScratchSet(
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
}
