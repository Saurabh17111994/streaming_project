package com.trading.compute.signaljob;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
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
 * tumbling window (OHLCV aggregate) → {@code feature_candles_15s} (Fluss LOG
 * sink) + {@code feature_candles_15s_current} (Fluss KV upsert sink, the
 * idempotent current-state twin — CANDLE-KV-REPLAY-001 P5) and → MVP signal
 * detection (Slice 2.1, DEC-034) → {@code Signal_Candidates} (Fluss KV upsert
 * sink). Business Logic operator internals (candidate lifecycle, max-one-active)
 * and Ranking/decision sinks stay disabled at those boundaries — postponed
 * with the ranking phase.
 *
 * <p>Checkpointing: EXACTLY_ONCE, pinned interval/timeout/max-concurrent
 * (REQ-FC-006); fixed-delay restart 3 × 30s. The sink is the Fluss
 * append writer ({@code RowDataSerializationSchema(true, true)} = append-only,
 * ignore delete — correct for a LOG table); the source consumes from the
 * earliest available offset on first start.
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
        // Read-only metadata preflight (CANDLE-KV-REPLAY-001 P4): prove the
        // deployed candle tables match the dual-write contract before any graph
        // is built — fail closed on contract drift, never write degraded.
        preflightTableContracts(config);

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(config.maxConcurrentCheckpoints());

        FlussSource<RowData> source = FlussSource.<RowData>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDatabase(config.database())
                .setTable(config.rawTable())
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        WatermarkStrategy<RowData> watermarks = CandleWatermarkStrategy.of(config);
        DataStream<RowData> ticks = env.fromSource(source, watermarks, "raw-table-1");

        DataStream<RowData> valid = ticks
                .flatMap(new RawValidationFunction(config))
                .returns(ticks.getType())
                .name("raw-validation");

        DataStream<RowData> deduped = valid
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .process(new FingerprintDedupFunction(config))
                .returns(ticks.getType())
                .name("fingerprint-dedup");

        SingleOutputStreamOperator<RowData> candles = deduped
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN))
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(config.candleWindowMs())))
                .allowedLateness(Duration.ofMillis(config.allowedLatenessMs()))
                .aggregate(new CandleAggregateFunction(), new CandleEmitFunction(config))
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("candle-15s");

        candles
                .sinkTo(FlussSink.<RowData>builder()
                        .setBootstrapServers(config.bootstrapServers())
                        .setDatabase(config.database())
                        .setTable(config.candleTable())
                        .setSerializationSchema(new RowDataSerializationSchema(true, true))
                        .build())
                .name("feature-candles-15s-sink");

        // Slice 2.1 (DEC-034): closed candles -> MVP signal detection ->
        // Signal_Candidates KV upsert. The KV sink uses
        // RowDataSerializationSchema(false, false): isAppendOnly=false maps
        // INSERT RowKinds to UPSERT operations for the KV writer; the writer
        // (Append vs Upsert) is chosen from the live table metadata fetched by
        // FlussSink.build() — fail-fast startup if the table is missing.
        DataStream<RowData> signals = candles
                .keyBy(row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN))
                .process(new SignalDetectionFunction(config))
                .returns(SignalCandidatesTableColumns.ROW_TYPE_INFO)
                .name("signal-detection");

        signals
                .sinkTo(FlussSink.<RowData>builder()
                        .setBootstrapServers(config.bootstrapServers())
                        .setDatabase(config.database())
                        .setTable(config.signalCandidatesTable())
                        .setSerializationSchema(new RowDataSerializationSchema(false, false))
                        .build())
                .name("signal-candidates-sink");

        // CANDLE-KV-REPLAY-001 P5: idempotent current-state twin. The LOG
        // sink above stays the immutable audit trail (append-only
        // serialization); this KV sink overwrites the same
        // (instrument_token, window_start) key via
        // RowDataSerializationSchema(false, false) — isAppendOnly=false maps
        // INSERT RowKinds to UPSERT operations for the KV writer — so
        // replay/restart duplicates converge instead of accumulating (the
        // 2026-08-10 replay appended ~550k duplicate rows to the LOG; the KV
        // twin would have stayed at one row per key). Added LAST to the
        // candles stream on purpose: operator IDs are derived from a BFS
        // node-counter, and emitting this sink earlier shifts the hash of the
        // stateful signal-detection operator (StreamGraphHasherV2) —
        // CHECKPOINT-RESTORE-001 pins detection's ID across the change.
        candles
                .sinkTo(FlussSink.<RowData>builder()
                        .setBootstrapServers(config.bootstrapServers())
                        .setDatabase(config.database())
                        .setTable(config.candleCurrentTable())
                        .setSerializationSchema(new RowDataSerializationSchema(false, false))
                        .build())
                .name("feature-candles-15s-current-kv-sink");

        return env;
    }

    /**
     * Read-only metadata preflight (CANDLE-KV-REPLAY-001 P4): opens a short
     * Fluss connection, checks the deployed candle tables against the
     * dual-write contract (LOG twin: no PK, instrument_token routing; KV twin:
     * PK exactly (instrument_token, window_start), same routing), and closes.
     * Any violation — or an unreachable cluster — fails startup before the
     * graph is built: the job never writes to tables that contradict the
     * contract it was compiled against.
     */
    static void preflightTableContracts(SignalJobConfig config) {
        org.apache.fluss.config.Configuration clientConf = new org.apache.fluss.config.Configuration();
        clientConf.setString("bootstrap.servers", config.bootstrapServers());
        try (org.apache.fluss.client.Connection conn =
                org.apache.fluss.client.ConnectionFactory.createConnection(clientConf)) {
            org.apache.fluss.metadata.TableInfo logInfo = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(config.database(), config.candleTable()))
                    .getTableInfo();
            CandleTableContractValidator.validateLogTable(logInfo);
            org.apache.fluss.metadata.TableInfo kvInfo = conn
                    .getTable(org.apache.fluss.metadata.TablePath.of(config.database(), config.candleCurrentTable()))
                    .getTableInfo();
            CandleTableContractValidator.validateCanonicalKvTable(kvInfo);
            LOG.info("signal-job: candle table contracts OK ({} LOG, {} KV)",
                    config.candleTable(), config.candleCurrentTable());
        } catch (CandleTableContractValidator.ContractViolation e) {
            throw e; // contract drift: fail closed, do not build a degraded graph
        } catch (Exception e) {
            throw new IllegalStateException(
                    "signal-job: candle-table preflight failed — is the dev Fluss cluster reachable at "
                            + config.bootstrapServers() + "? (" + e.getMessage() + ")", e);
        }
    }
}
