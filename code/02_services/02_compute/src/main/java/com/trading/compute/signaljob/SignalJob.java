package com.trading.compute.signaljob;

import com.trading.common.schema.CandleTableSchema;
import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.data.RowData;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Signal job — compute path (docs/08_implementation/04-signal-job.md).
 *
 * <p>Topology: {@code raw_table_1} (Fluss LOG source, full offsets) → raw
 * schema/validity gate → bounded fingerprint dedup → 15-second event-time
 * tumbling window (OHLCV aggregate) → {@code feature_candles_15s} (Fluss KV
 * upsert sink — user requirement 2026-08-13: candle tables are KV-only, no
 * LOG+KV twin) → MVP signal detection (Slice 2.1, DEC-034) → signal dual-sink
 * (DEC-035): {@code Signal_Candidates} (Fluss LOG append, every signal) and
 * {@code Signal_Candidates_current} (Fluss KV upsert behind the
 * canonical-signal filter). Business Logic operator internals (candidate
 * lifecycle, max-one-active) and Ranking/decision sinks stay disabled at
 * those boundaries — postponed with the ranking phase.
 *
 * <p>Checkpointing: EXACTLY_ONCE, pinned interval/timeout/max-concurrent
 * (REQ-FC-006); fixed-delay restart 3 × 30s. All sinks stay inside
 * {@code StallGuardedSink} (hang containment, identical on all sinks). The
 * signal LOG sink uses {@code RowDataSerializationSchema(true, true)}
 * (append-only, ignore delete — correct for a LOG table); the candle KV and
 * signal current-state KV sinks use
 * {@code RowDataSerializationSchema(false, false)} (INSERT → UPSERT for the
 * KV writer). The source consumes from the earliest available offset on
 * first start.
 */
public final class SignalJob {

    private static final Logger LOG = LoggerFactory.getLogger(SignalJob.class);

    private SignalJob() {}

    public static void main(String[] args) throws Exception {
        SignalJobConfig config = SignalJobConfig.fromEnv();
        LOG.info("signal-job: starting compute path with {}", config);
        run(config);
    }

    public static void run(SignalJobConfig config) throws Exception {
        // Startup-mode gate (CANDLE-KV-REPLAY-001 A3.3/A3.4): the config was
        // already validated by fromEnv(); log the mode for the operator and
        // ship it as a gauge so the run's startup is observable post hoc.
        // FULL_REPLAY is a break-glass mode — surface it at WARN, not INFO.
        if (config.startupMode() == SignalJobConfig.StartupMode.FULL_REPLAY) {
            LOG.warn("signal-job: startup mode = {} (restore={}, fullReplay={}) "
                    + "— offset-0 full replay accepted via ALLOW_FULL_REPLAY (A3.4)",
                    config.startupMode(), config.stateRecoveryPath() != null, config.allowFullReplay());
        } else {
            LOG.info("signal-job: startup mode = {} (restore={}, fullReplay={})",
                    config.startupMode(), config.stateRecoveryPath() != null, config.allowFullReplay());
        }
        ComputeOtlpEmitter.recordStartupMode(
                config.startupMode() == SignalJobConfig.StartupMode.RESTORE ? 0 : 1);

        // Tracker 14 P8.0 box 828: ship the startup-mode event to the
        // trading_alerts stream (synchronous — the periodic emitter flush
        // never runs for these client-side lifecycle events under
        // `flink run -d`). Best-effort: collector outage never fails the job.
        ComputeOtlpEmitter.emitAlertLog(config.otelCollectorHost(),
                config.startupMode() == SignalJobConfig.StartupMode.FULL_REPLAY ? "WARN" : "INFO",
                "startup-mode",
                "mode=" + config.startupMode() + " restore=" + (config.stateRecoveryPath() != null)
                        + " fullReplay=" + config.allowFullReplay());

        // Tracker 14 P8.0/831 — resource attributes (environment, host,
        // deployment version, job name, execution mode) ride every payload so
        // OpenObserve queries can slice by env/host/version. Only known-safe
        // config fields + hostname; never credentials.
        ComputeOtlpEmitter.configureResourceAttributes(
                "deployment.environment", config.deploymentEnv(),
                "host.name", hostName(),
                "deployment.version", config.configurationVersion(),
                "job.name", "signal-job",
                "flink.execution.mode", "embedded");

        // Process rule 2 (2026-08-10): ship the schema-version rejection counter
        // to OpenObserve via the OTel collector (delta per 10 s flush). Static
        // holder + same-JVM drain is exact for the embedded dev run.
        ComputeOtlpEmitter otlp = new ComputeOtlpEmitter(config.otelCollectorHost());
        otlp.start();

        StreamExecutionEnvironment env = buildTopology(config);
        env.execute("signal-job-compute");
    }

    /**
     * Builds the full compute topology (source → validation → dedup → window →
     * candle sinks → signal detection → candidates sink) <b>without
     * executing</b>.
     *
     * <p>Shared by {@link #run(SignalJobConfig)} and the offline
     * JobGraph/operator-ID comparison tool ({@code JobGraphDump}, P6 of
     * CANDLE-KV-REPLAY-001): the restore-compatibility evidence compares this
     * graph before and after the tracker change, so both dumps must be built
     * by the same code path the running job uses.
     */
    public static StreamExecutionEnvironment buildTopology(SignalJobConfig config) {
        // Read-only metadata preflight (tracker 14 P1 / re-scoped P2): prove
        // the deployed tables (candle KV, signal LOG, signal current-state KV)
        // match the contracts the write paths rely on before any graph is
        // built — fail closed on contract drift, never write degraded.
        try {
            preflightTableContracts(config);
        } catch (TableContractValidator.ContractViolation e) {
            // Tracker 14 P8.0 box 828: record the fail-closed startup event on
            // the trading_alerts stream before rethrowing — the job exits
            // right after, so this is the only chance to ship it.
            ComputeOtlpEmitter.emitAlertLog(config.otelCollectorHost(), "ERROR",
                    "schema-preflight-failed",
                    String.valueOf(e.getMessage()));
            throw e;
        }

        // Flink 2.x configures restart strategies declaratively via Configuration —
        // the programmatic RestartStrategies API was removed (verified against
        // flink-core 2.2.1). Create the environment from that configuration.
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.FIXED_DELAY.getMainValue());
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                config.restartMaxAttempts());
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(config.restartDelayMs()));
        // Checkpoint storage is a deployment property (flink-conf.yaml state.checkpoints.dir
        // in the dist). Flink 2.2.1 removed CheckpointConfig.setCheckpointStorage; the
        // declarative Configuration route is the only way to point local/dev runs at a
        // durable directory instead of the 5 MiB-capped JobManager-heap default.
        if (config.checkpointDir() != null) {
            flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointDir());
        }
        // State restore (STATE_RECOVERY_PATH): without an explicit restore the
        // job starts from offset 0 and replays the whole LOG backlog, which blows
        // the pinned checkpoint contract (REQ-FC-006: 10 s interval / 30 s timeout /
        // 1 concurrent) and cascades to JobManager death. A restart MUST resume
        // from the last checkpoint of the previous run. Restore-from-checkpoint
        // works with the same path key as savepoints (StreamGraphGenerator reads
        // StateRecoveryOptions.SAVEPOINT_PATH -> SavepointRestoreSettings).
        if (config.stateRecoveryPath() != null) {
            flinkConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, config.stateRecoveryPath());
        }
        // Production runtime options (tracker 14 P4.1/P4.2): state backend,
        // incremental checkpoints, RocksDB local dirs / managed memory, savepoint
        // directory, parallelism. Backend + storage are Configuration-driven in
        // Flink 2.2.1; applied before the environment is created.
        applyRuntimeOptions(config, flinkConfig);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.setParallelism(config.parallelism());

        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(config.maxConcurrentCheckpoints());
        // A deliberate cancel/restart must retain the completed checkpoint named by
        // STATE_RECOVERY_PATH; deleting it would silently force an unsafe offset-0 replay.
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        FlussSource<RowData> source = FlussSource.<RowData>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDatabase(config.database())
                .setTable(config.rawTable())
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        WatermarkStrategy<RowData> watermarks = CandleWatermarkStrategy.of(config);
        DataStream<RowData> ticks = env.fromSource(source, watermarks, "raw-table-1")
                .uid("raw-table-1");

        DataStream<RowData> valid = ticks
                .flatMap(new RawValidationFunction(config))
                .returns(ticks.getType())
                .name("raw-validation")
                .uid("raw-validation");

        SingleOutputStreamOperator<RowData> deduped = valid
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .process(new FingerprintDedupFunction(config, () ->
                        FlussFingerprintDedupStateStore.open(
                                config.bootstrapServers(), config.database(),
                                config.dedupStateTable(),
                                java.time.Duration.ofMillis(config.dedupCleanupIntervalMs()))))
                .returns(ticks.getType())
                .name("fingerprint-dedup")
                .uid("fingerprint-dedup");

        // DEC-038 (2026-08-15): the authoritative dedup set lives in Fluss.
        // First-seen fingerprints leave the dedup operator via the side output
        // and are durably upserted to fingerprint_dedup through the batched
        // writer (DEDUP_WRITE_BATCH_MS / DEDUP_WRITE_BATCH_SIZE cadence) + the
        // FlussSink (INSERT -> UPSERT; barrier-aligned). The sink is wrapped in
        // StallGuardedSink like every other Fluss sink (box 682/116).
        deduped
                .getSideOutput(FingerprintDedupFunction.DEDUP_WRITE_OUTPUT)
                .keyBy(row -> 0L)
                .process(new FingerprintDedupWriterFunction(config))
                .name("fingerprint-dedup-writer")
                .uid("fingerprint-dedup-writer")
                .setParallelism(1)
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.dedupStateTable())
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        config.sinkWriteStallTimeoutMs()))
                .name("fingerprint-dedup-sink")
                .uid("fingerprint-dedup-sink");

        SingleOutputStreamOperator<RowData> candles = deduped
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(config.candleWindowMs())))
                .allowedLateness(Duration.ofMillis(config.allowedLatenessMs()))
                .aggregate(new CandleAggregateFunction(), new CandleEmitFunction(config))
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("candle-15s")
                .uid("candle-15s");

        // Tracker 14 box 682/116 (2026-08-12): the candle sink is
        // wrapped in StallGuardedSink — a Flink-side watchdog
        // that runs every delegate write/flush/close on a worker thread and
        // bounds the CALL ITSELF at SINK_WRITE_STALL_TIMEOUT_MS. This is
        // required because the Fluss client has two unbounded hang points a
        // post-hoc check cannot see (bytecode-verified in
        // fluss-client-0.9.1-incubating): flush() blocks forever in
        // RecordAccumulator.awaitFlushCompletion() (latch never counts down
        // while the deleted table's batch stays undrained) and close() blocks
        // forever in awaitTermination(Long.MAX_VALUE) — the sender's shutdown
        // drain loop needs forceClose=true, which close() only sets AFTER
        // awaitTermination returns (circular deadlock). On timeout the guard
        // interrupts the worker (both hang points exit fast on interrupt) and
        // throws, failing the task so the configured restart policy drives the
        // job to terminal FAILED instead of cycling FAILING forever.
        // client.request-timeout (spike-verified: FlussSinkBuilder.build() ->
        // Configuration.fromMap(configOptions) -> ConnectionFactory, so
        // sink-scoped options reach the writer client) and
        // client.writer.retries=2 (default Integer.MAX_VALUE, verified in
        // Sender.canRetry: attempts < retries) bound transient failures; the
        // deleted-table case never consults retries (its batch never fails),
        // so the interrupt-based call bound is the actual unblock.
        candles
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.candleTable())
                                // KV upsert: feature_candles_15s is a KV table
                                // (PK instrument_token, window_start) — the
                                // (false, false) RowDataSerializationSchema maps
                                // INSERT RowKinds to UPSERTs so replay/restart
                                // re-emits converge instead of appending
                                // duplicates (user requirement 2026-08-13:
                                // candle tables are KV-only, no LOG+KV twin).
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        config.sinkWriteStallTimeoutMs()))
                .name("feature-candles-15s-sink")
                .uid("feature-candles-15s-sink");

        // Slice 2.1 (DEC-034): closed candles -> MVP signal detection ->
        // signal dual-sink (DEC-035, tracker 14 re-scoped P2).
        //
        // (a) Signal_Candidates LOG: append-only RowDataSerializationSchema
        // (true, true) — every emitted signal is an immutable audit row.
        // (b) Signal_Candidates_current KV: the canonical-signal filter keeps
        // only the pinned canonical identity (schema_version +
        // strategy_id + strategy_version + rule_id) and the
        // RowDataSerializationSchema(false, false) maps INSERT RowKinds to
        // UPSERTs; the writer (Append vs Upsert) is chosen from the live
        // table metadata fetched by FlussSink.build() — fail-fast startup if
        // the table is missing or is not a KV table.
        //
        // Both sinks stay inside StallGuardedSink (tracker 14 box 682/116,
        // identical on ALL sinks): the Fluss client has two unbounded hang
        // points only an interrupt-bounded call guard can see; on timeout the
        // guard fails the task so the restart policy drives the job to
        // terminal FAILED instead of cycling FAILING forever.
        DataStream<RowData> signals = candles
                .keyBy(row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN))
                .process(new SignalDetectionFunction(config))
                .returns(SignalCandidatesTableColumns.ROW_TYPE_INFO)
                .name("signal-detection")
                .uid("signal-detection");

        signals
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.signalCandidatesTable())
                                .setSerializationSchema(new RowDataSerializationSchema(true, true))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        config.sinkWriteStallTimeoutMs()))
                .name("signal-candidates-sink")
                .uid("signal-candidates-sink");

        signals
                .filter(new CanonicalSignalFilterFunction())
                .name("canonical-signal-filter")
                .uid("canonical-signal-filter")
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.signalCurrentTable())
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        config.sinkWriteStallTimeoutMs()))
                .name("signal-candidates-current-sink")
                .uid("signal-candidates-current-sink");

        return env;
    }

    /**
     * Production runtime options (tracker 14 P4.1/P4.2), extracted from the
     * config into the Flink {@link Configuration}: state backend (rocksdb in
     * production, hashmap dev-only — validated by
     * {@code SignalJobConfig.from}), incremental checkpoints for RocksDB,
     * RocksDB local state dirs + managed memory, the savepoint directory, and
     * the explicit checkpoint directory (kept from the caller, above).
     *
     * <p>Never sets {@code allowNonRestoredState} — a restore that cannot
     * fully match the graph fails closed (P4.3, CHECKPOINT-RESTORE-001), it
     * does not degrade to a silent full replay.
     *
     * <p>Package-visible so {@code RuntimeOptionsTest} can assert the exact
     * Configuration a run would use, without a Flink cluster.
     */
    static void applyRuntimeOptions(SignalJobConfig config, Configuration flinkConfig) {
        if ("rocksdb".equals(config.stateBackend())) {
            // Shortcut names per StateBackendOptions: 'rocksdb' (or 'hashmap').
            flinkConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            // Incremental checkpoints are enabled only on the RocksDB backend
            // (Flink ignores them on heap state) and only for the keyed
            // MapState + timer state this graph uses — both fully supported.
            flinkConfig.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
            if (config.stateBackendLocalDirs() != null) {
                // RocksDBOptions.LOCAL_DIRECTORIES ("state.backend.rocksdb.localdir",
                // singular) is the live key in Flink 2.2.1; the older
                // "state.backend.rocksdb.local_directories" key is dead in this
                // version and would silently drop the fast-disk pin (tracker 14
                // P4.1 — verified against the pinned
                // flink-statebackend-rocksdb-2.2.1.jar).
                flinkConfig.setString(
                        "state.backend.rocksdb.localdir", config.stateBackendLocalDirs());
            }
            if (!config.stateBackendManagedMemory()) {
                flinkConfig.setString("state.backend.rocksdb.memory.managed", "false");
            }
            // Tracker 14 box 906 (2026-08-12): export RocksDB native-memory
            // gauges via the per-property boolean keys (verified against
            // RocksDBProperty in the pinned flink-statebackend-rocksdb-2.2.1
            // jar: block-cache-usage / cur-size-all-mem-tables /
            // estimate-table-readers-mem are valid enum kebab names). The
            // gauges register on the keyed-state operator metric group and
            // land on the TM reporter output as
            // flink_taskmanager_job_task_operator_<state.backend.rocksdb.<prop>>
            // series. RocksDB-only by construction — the hashmap branch sets
            // none of these keys.
            flinkConfig.setString("state.backend.rocksdb.metrics.block-cache-usage", "true");
            flinkConfig.setString(
                    "state.backend.rocksdb.metrics.cur-size-all-mem-tables", "true");
            flinkConfig.setString(
                    "state.backend.rocksdb.metrics.estimate-table-readers-mem", "true");
        } else {
            flinkConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        }
        if (config.savepointDir() != null) {
            flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, config.savepointDir());
        }
        // Tracker 14 P4.2 — object-store (S3/R2) checkpoint access. The
        // endpoint/credentials/region go into the Flink Configuration ONLY
        // when a checkpoint/savepoint URI is an object-store URI (config
        // validation in SignalJobConfig.s3Endpoint already failed closed
        // otherwise). Credentials come from secret injection via env — never
        // committed files — and the effective-backend log below prints URI
        // schemes only, never the endpoint path or keys.
        if (config.s3Endpoint() != null) {
            flinkConfig.setString("fs.s3a.endpoint", config.s3Endpoint());
            flinkConfig.setString("fs.s3a.access.key", config.s3AccessKey());
            flinkConfig.setString("fs.s3a.secret.key", config.s3SecretKey());
            flinkConfig.setString("fs.s3a.endpoint.region", config.s3Region());
            flinkConfig.setString("fs.s3a.path.style.access", String.valueOf(config.s3PathStyle()));
            flinkConfig.setString("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        }
        // Effective-backend log WITHOUT secrets: the checkpoint URI is printed
        // as its scheme only, never the full path (credentials may be embedded
        // in S3 URIs — tracker 14 P4.2 "never committed files").
        String cpScheme = config.checkpointDir() == null ? "none"
                : config.checkpointDir().substring(0, config.checkpointDir().indexOf(':'));
        LOG.info("signal-job: effective state backend = {} (dev={}, incremental={}), "
                + "checkpoint URI class = {}, savepoint URI class = {}, parallelism = {}",
                config.stateBackend(), config.deploymentEnv(),
                "rocksdb".equals(config.stateBackend())
                        && flinkConfig.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS),
                cpScheme,
                config.savepointDir() == null ? "none"
                        : config.savepointDir().substring(0, config.savepointDir().indexOf(':')),
                config.parallelism());
    }

    /**
     * Read-only metadata preflight (tracker 14 P1 — CANDLE-SCHEMA-002;
     * tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001): opens a short Fluss
     * connection and checks the three deployed tables the write paths rely
     * on — the candle KV (PK exactly [instrument_token, window_start],
     * instrument_token routing, 16 buckets, exact 15-column v2 schema), the
     * signal LOG (no PK, instrument_token routing, 16 buckets, exact
     * 22-column v3 schema), and the signal current-state KV (PK exactly
     * [instrument_token], instrument_token routing, 16 buckets, the same
     * 22-column schema) — then closes. Any
     * violation — or an unreachable cluster — fails startup before the graph
     * is built: the job never writes to a table that contradicts the contract
     * it was compiled against.
     */
    static void preflightTableContracts(SignalJobConfig config) {
        org.apache.fluss.config.Configuration clientConf = new org.apache.fluss.config.Configuration();
        clientConf.setString("bootstrap.servers", config.bootstrapServers());
        try (org.apache.fluss.client.Connection conn =
                org.apache.fluss.client.ConnectionFactory.createConnection(clientConf)) {
            org.apache.fluss.metadata.TableInfo candleKv = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(config.database(), config.candleTable()))
                    .getTableInfo();
            TableContractValidator.validateCandleKvTable(candleKv);
            org.apache.fluss.metadata.TableInfo signalLog = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.signalCandidatesTable()))
                    .getTableInfo();
            TableContractValidator.validateSignalLogTable(signalLog);
            org.apache.fluss.metadata.TableInfo signalCurrent = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.signalCurrentTable()))
                    .getTableInfo();
            TableContractValidator.validateSignalCurrentKvTable(signalCurrent);
            // DEC-038: the authoritative dedup set lives in Fluss, so the
            // fingerprint_dedup table is a hard startup dependency — fail
            // closed on drift, never run with an empty/mismatched dedup set
            // (SIG-STATE-003).
            org.apache.fluss.metadata.TableInfo dedupState = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(
                            config.database(), config.dedupStateTable()))
                    .getTableInfo();
            TableContractValidator.validateFingerprintDedupTable(dedupState);
            // SCH-19 (machinery): when the decision dual-sink is enabled, the
            // Trade_Decisions LOG + trade_instruction_state KV index must
            // match the contracts the write paths rely on before the graph is
            // built — fail closed on drift, never write degraded. Disabled by
            // default: the ranking feed (Slice 3) does not exist yet.
            if (config.tradeDecisionsEnabled()) {
                org.apache.fluss.metadata.TableInfo tradeLog = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.tradeDecisionsTable()))
                        .getTableInfo();
                TableContractValidator.validateTradeDecisionsLogTable(tradeLog);
                org.apache.fluss.metadata.TableInfo tradeIndex = conn
                        .getTable(org.apache.fluss.metadata.TablePath.of(
                                config.database(), config.tradeInstructionStateTable()))
                        .getTableInfo();
                TableContractValidator.validateTradeInstructionStateKvTable(tradeIndex);
                LOG.info("signal-job: trade-decisions LOG contract OK ({})",
                        config.tradeDecisionsTable());
                LOG.info("signal-job: {}", TableContractValidator.schemaReport(
                        tradeLog, TradeDecisionsTableColumns.COLUMN_NULLABLE_IN_DDL));
                LOG.info("signal-job: trade-instruction-state KV contract OK ({})",
                        config.tradeInstructionStateTable());
            }
            // Log the validated schema reports — exact live columns/types
            // (and the DDL-vs-live nullability divergence where Fluss does
            // not carry NOT NULL) as startup evidence.
            LOG.info("signal-job: candle table contract OK ({} KV)", config.candleTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            candleKv, CandleTableSchema.COLUMN_NULLABLE_IN_DDL));
            LOG.info("signal-job: signal LOG contract OK ({})", config.signalCandidatesTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            signalLog, SignalCandidatesTableColumns.COLUMN_NULLABLE_IN_DDL));
            LOG.info("signal-job: signal current-state KV contract OK ({})",
                    config.signalCurrentTable());
            LOG.info("signal-job: {}",
                    TableContractValidator.schemaReport(
                            signalCurrent, SignalCandidatesTableColumns.COLUMN_NULLABLE_IN_DDL));
        } catch (TableContractValidator.ContractViolation e) {
            throw e; // contract drift: fail closed, do not build a degraded graph
        } catch (Exception e) {
            throw new IllegalStateException(
                    "signal-job: table preflight failed — is the dev Fluss cluster reachable at "
                            + config.bootstrapServers() + "? (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Best-effort host name for the {@code host.name} resource attribute
     * (tracker 14 P8.0/831): the container/OS hostname, never a secret.
     * Fallback chain: {@code HOSTNAME} env (containers/shells) →
     * {@code InetAddress} → {@code "unknown"} — a resolve failure must not
     * fail the job (telemetry is off the critical path).
     */
    static String hostName() {
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env;
        }
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            return "unknown";
        }
    }
}
