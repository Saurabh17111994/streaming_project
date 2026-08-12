package com.trading.compute.tools;

import com.trading.compute.signaljob.CandleTableColumns;
import com.trading.compute.signaljob.CanonicalCandlePolicy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.fluss.flink.catalog.FlinkCatalog;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.utils.DataLakeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker 14 P3.3 (box 419): production migration as a <b>bounded Flink batch
 * job</b> — the Table-API twin of {@link CandleMigrationTool}. Same evidence
 * contracts, no CLI in-heap cap: the two-pass keyed aggregation is the
 * disk-backed bounded sort-merge variant, keyed by
 * {@code (instrument_token, window_start)} exactly as box 419 requires.
 *
 * <p><b>Source (P3.2 union read).</b> The DataStream-API {@code FlussSource}
 * is hardcoded UNBOUNDED (streaming=true in {@code FlussSourceBuilder.build()})
 * and Flink 2.2 rejects it under {@code execution.runtime-mode=BATCH}; the only
 * bounded read is the Table API source ({@code FlinkTableFactory} /
 * {@code FlinkTableSource} with {@code streaming=false}), which requires a
 * lake-enabled LOG table (≥1 Iceberg snapshot) and reads the hybrid
 * lake+log union — the same coverage the CLI's {@code CANDLE_MIGRATION_UNION_READ}
 * gate demanded. The LOG boundary is {@code OffsetsInitializer.latest()} at
 * enumeration: rows appended during the run stay outside the boundary
 * (single-scan cutover semantics, same as the CLI's "writers must be stopped"
 * contract).
 *
 * <p><b>Pipeline.</b> {@code canonical filter} (schema_version + canonical
 * algorithm/configuration pair; null key fails closed, P3.4) →
 * <b>pass 1</b> {@code keyBy(token, windowStart, rowHash)} reduce to the
 * MAX({@code output_ts}) row (identical business rows merge — replay
 * convergence) → <b>pass 2</b> {@code keyBy(token, windowStart)}
 * {@link KeyDecisionProcessFunction} per-key candidate list (cap
 * {@link #MAX_CONFLICT_CANDIDATES} + truncated flag, same as the CLI) →
 * {@link Decision} classification (CLEAN / APPROVED / UNACCEPTED / STALE) →
 * upsert rows (CLEAN+APPROVED) flow to the KV destination in {@code load}
 * mode; decision metadata (a RowData-free POJO, never Kryo-serialized) feeds
 * the evidence-file renderer and the {@link MigrationGateSink}.
 *
 * <p><b>Gate (CLI exit semantics).</b> The gate sink is a
 * {@link RichSinkFunction}: {@code open()} loads the approval file, {@code close()}
 * computes {@code notFound = approvals − seenKeys} and throws
 * {@link MigrationBlockedException} when any UNACCEPTED (exit 2 parity), STALE
 * or NOT_FOUND (exit 1 parity) key exists — the job ends FAILED. There is no
 * end-of-input callback for plain operators in batch, so the gate must live in
 * a sink's {@code close()} (unlike the CLI's per-bucket early abort, the whole
 * bounded input is consumed before the gate fires; the KV sink is only attached
 * in {@code load} mode and its flush is idempotent-by-PK, so a gated run leaves
 * no partial damage).
 *
 * <p><b>Environment</b> (same names as {@link CandleMigrationTool.Config}):
 * {@code FLUSS_BOOTSTRAP_SERVERS}, {@code CANDLE_TABLE}, {@code CANDLE_CURRENT_TABLE},
 * {@code SCHEMA_VERSION}, {@code ALGORITHM_VERSION}, {@code CONFIGURATION_VERSION},
 * {@code CANDLE_MIGRATION_ACCEPT_KEYS_FILE}; batch-only:
 * {@code CANDLE_MIGRATION_MODE=audit|load} (default {@code audit}),
 * {@code CANDLE_MIGRATION_REPORT_DIR} (default {@code candle-migration-reports}).
 *
 * <p>Parallelism is pinned to 1 everywhere: the migration is a single-scan
 * cutover and the notFound gate needs every approval key's absence visible in
 * one task.
 */
public final class CandleMigrationBatchJob {

    private static final Logger LOG = LoggerFactory.getLogger(CandleMigrationBatchJob.class);

    /** Per-key candidate cap, parity with {@code CandleMigrationTool.KeyAgg}. */
    static final int MAX_CONFLICT_CANDIDATES = 64;

    /** Reject DDL identifiers that could smuggle SQL (env → DDL). */
    private static final String TABLE_NAME_RE = "[A-Za-z_][A-Za-z0-9_]*";

    static final OutputTag<RowData> NON_CANONICAL_OUT =
            new OutputTag<>("migration-non-canonical", CandleTableColumns.ROW_TYPE_INFO);
    /** Per-key decision metadata (RowData-free POJO — never Kryo'd). */
    static final OutputTag<KeyDecisionMeta> DECISION_OUT =
            new OutputTag<>("migration-decisions", Types.POJO(KeyDecisionMeta.class));

    private CandleMigrationBatchJob() {}

    /**
     * Lake-catalog properties for the {@code FlinkCatalog} supplier. The fluss
     * connector prepends {@code table.datalake.} to each key; {@code
     * DataLakeUtils.extractLakeCatalogProperties} strips {@code
     * table.datalake.iceberg.} and {@code HadoopUtils} (prefix {@code
     * iceberg.hadoop.}) strips the rest, so each entry lands in the
     * S3AFileSystem {@code Configuration} as {@code fs.s3a.*}. Verified against
     * fluss-flink-common 0.9.1-incubating ({@code FlinkCatalog.getTable},
     * datalake-enabled branch — plain table names take this path and get the
     * {@code table.datalake.} prepend at line 364) and fluss-lake-iceberg
     * 0.9.1-incubating ({@code HadoopUtils.FLUSS_CONFIG_PREFIXES}):
     * {@code FlinkCatalog.getTable} → {@code LakeSourceUtils.createLakeSource}
     * → {@code DataLakeUtils} → {@code HadoopUtils}. Without these pins the R2
     * lake reader had an infinite socket timeout and a wedged connection
     * blocked the audit forever (tracker 14 R2 lake-read stall 2026-08-12).
     * Never put credentials here — the keys are passed to the catalog
     * supplier, not the env.
     */
    static Map<String, String> lakeCatalogProperties() {
        return lakeCatalogProperties(
                System.getenv("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS"),
                System.getenv("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS"));
    }

    /**
     * Pure two-arg variant for hermetic tests (no env access); null raw means
     * missing -> default 30000.
     */
    static Map<String, String> lakeCatalogProperties(
            String connectionTimeoutRaw, String socketTimeoutRaw) {
        // In hadoop-aws 3.3.x (fs-s3.jar) S3AUtils.initConnectionSettings maps
        // fs.s3a.connection.timeout -> SDK ClientConfiguration.setSocketTimeout
        // (SOCKET READ timeout; S3A default 200000) and
        // fs.s3a.connection.establish.timeout -> setConnectionTimeout (TCP
        // connect; default 50000). fs.s3a.socket.timeout is NOT read — it would
        // be a silent no-op, so the second env pin targets the connect timeout.
        return Map.of(
                "iceberg.iceberg.hadoop.fs.s3a.connection.timeout",
                s3TimeoutMs("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS", connectionTimeoutRaw),
                "iceberg.iceberg.hadoop.fs.s3a.connection.establish.timeout",
                s3TimeoutMs("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS", socketTimeoutRaw));
    }

    /**
     * S3A timeout (ms) from env: default 30000, range [1000, 300000]. Missing
     * uses the default; non-numeric, zero or negative values fail startup — a
     * silent fallback would re-expose the unbounded-stall failure mode this
     * pin removes.
     */
    static String s3TimeoutMs(String envName) {
        return s3TimeoutMs(envName, System.getenv(envName));
    }

    /** Pure parse/validate (testable without env access); null raw = missing. */
    static String s3TimeoutMs(String envName, String raw) {
        if (raw == null) {
            raw = "30000";
        }
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("candle-migration: " + envName
                    + " must be an integer milliseconds value, got '" + raw + "'");
        }
        if (value < 1000 || value > 300000) {
            throw new IllegalArgumentException("candle-migration: " + envName
                    + " must be between 1000 and 300000 ms, got " + value);
        }
        return Long.toString(value);
    }

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        Map<String, CandleMigrationTool.AcceptEntry> approvals =
                CandleMigrationTool.loadAcceptKeys(cfg.acceptKeysFile);

        System.out.println("CANDLE_MIGRATION_MODE=" + cfg.mode);
        System.out.println("CANDLE_MIGRATION_SOURCE=" + cfg.sourceTable);
        System.out.println("CANDLE_MIGRATION_FILTER=schema_version=" + cfg.schemaVersion
                + ",algorithm_version=" + cfg.algorithmVersion
                + ",configuration_version=" + cfg.configurationVersion);
        System.out.println("CANDLE_MIGRATION_UNION_READ=ENABLED");
        System.out.println("CANDLE_MIGRATION_LAKE_FORMAT=iceberg");
        Map<String, String> lakeProps = lakeCatalogProperties();
        System.out.println("CANDLE_MIGRATION_S3_CONNECTION_TIMEOUT_MS="
                + lakeProps.get("iceberg.iceberg.hadoop.fs.s3a.connection.timeout"));
        System.out.println("CANDLE_MIGRATION_S3_SOCKET_TIMEOUT_MS="
                + lakeProps.get("iceberg.iceberg.hadoop.fs.s3a.connection.establish.timeout"));
        System.out.println("CANDLE_MIGRATION_READ_TS=" + Instant.now());
        System.out.println("CANDLE_MIGRATION_CUTOVER=single-scan boundary "
                + "(OffsetsInitializer.latest at enumeration); rows appended during the run "
                + "stay outside the boundary");
        if (!approvals.isEmpty()) {
            System.out.println("CANDLE_MIGRATION_DEV_EXCEPTION=1");
            System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS_FILE=" + cfg.acceptKeysFile);
            System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS=" + approvals.size());
            LOG.warn("candle-migration: approval file supplied ({} entries) — this load is a "
                    + "DEV_EXCEPTION (2026-08-10 replay-incident keys), not clean production "
                    + "evidence (tracker 14 P3.1)", approvals.size());
        }

        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerCatalog("fluss", new FlinkCatalog(
                "fluss", "default", cfg.bootstrap,
                CandleMigrationBatchJob.class.getClassLoader(), Map.of(), () -> lakeProps));
        tEnv.useCatalog("fluss");
        tEnv.useDatabase("default");

        if (!cfg.sourceTable.matches(TABLE_NAME_RE)) {
            throw new IllegalArgumentException("candle-migration: CANDLE_TABLE '" + cfg.sourceTable
                    + "' is not a safe table identifier (tracker 14 P3.3)");
        }

        // The migration source is provisioned by the platform DDL — the batch
        // job must NOT create it (fail fast when missing). FlinkCatalog.getTable
        // resolves the existing table and injects connector + bootstrap + lake
        // options from the server-side table properties.
        Table sourceTable = tEnv.from(cfg.sourceTable);

        // R2 lake-read stall diagnostic (tracker 14): print the effective hadoop
        // conf the lake source builds from the PLANNER-RESOLVED table options —
        // the exact chain LakeSourceUtils -> DataLakeUtils.extractLakeCatalogProperties
        // -> HadoopUtils.getHadoopConfiguration used at read time. The supplier
        // pins must be visible here or the S3A reads stay unbounded.
        try {
            org.apache.flink.table.catalog.Catalog resolvedCatalog =
                    tEnv.getCatalog("fluss").get();
            org.apache.flink.table.catalog.CatalogBaseTable resolvedTable =
                    resolvedCatalog.getTable(
                            new org.apache.flink.table.catalog.ObjectPath(
                                    "default", cfg.sourceTable));
            Map<String, String> resolvedOptions = resolvedTable.getOptions();
            System.out.println("CANDLE_MIGRATION_RESOLVED_OPTION_COUNT=" + resolvedOptions.size());
            for (String k : new String[]{
                    "table.datalake.iceberg.iceberg.hadoop.fs.s3a.connection.timeout",
                    "table.datalake.iceberg.iceberg.hadoop.fs.s3a.connection.establish.timeout"}) {
                System.out.println("CANDLE_MIGRATION_RESOLVED_OPTION_" + k + "="
                        + resolvedOptions.get(k));
            }
            Map<String, String> lakeCatalogProps =
                    DataLakeUtils.extractLakeCatalogProperties(
                            org.apache.fluss.config.Configuration.fromMap(resolvedOptions));
            Class<?> hadoopUtils =
                    Class.forName("org.apache.fluss.lake.iceberg.conf.HadoopUtils");
            Object hadoopConf = hadoopUtils.getMethod("getHadoopConfiguration",
                    org.apache.fluss.config.Configuration.class)
                    .invoke(null, org.apache.fluss.config.Configuration.fromMap(lakeCatalogProps));
            java.lang.reflect.Method confGet = hadoopConf.getClass()
                    .getMethod("get", String.class, String.class);
            System.out.println("CANDLE_MIGRATION_EFFECTIVE_CONNECTION_TIMEOUT_MS="
                    + confGet.invoke(hadoopConf, "fs.s3a.connection.timeout", "<unset>"));
            System.out.println("CANDLE_MIGRATION_EFFECTIVE_ESTABLISH_TIMEOUT_MS="
                    + confGet.invoke(hadoopConf, "fs.s3a.connection.establish.timeout", "<unset>"));
        } catch (Throwable t) {
            System.out.println("CANDLE_MIGRATION_EFFECTIVE_CONF_DIAGNOSTIC_FAILED="
                    + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
        DataStream<RowData> rows = tEnv.toDataStream(sourceTable)
                .map(CandleMigrationBatchJob::toRowData)
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("migration-source-lake-batch");

        wire(env, cfg, rows);
        env.execute("candle-migration-batch");
    }

    /**
     * Pipeline + sinks below the source. Kept separate so tests can wire the
     * same operators against a canned bounded source.
     */
    static void wire(StreamExecutionEnvironment env, Config cfg, DataStream<RowData> rows) {
        SingleOutputStreamOperator<RowData> canonical = rows
                .process(new CanonicalFilterProcessFunction(cfg))
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("migration-canonical-filter");
        DataStream<RowData> nonCanonical = canonical.getSideOutput(NON_CANONICAL_OUT);

        // Pass 1: identical business rows (same token, windowStart, rowHash) merge
        // to the MAX(output_ts) row — replay duplicates converge (P3.4). Named
        // KeySelector classes (not lambdas) so Flink can extract the key types.
        DataStream<RowData> distinct = canonical
                .keyBy(new KeySelector<RowData, Tuple3<Long, Long, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple3<Long, Long, String> getKey(RowData row) {
                        return Tuple3.of(
                                row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                                row.getLong(CandleTableColumns.WINDOW_START),
                                rowHash(row));
                    }
                })
                .reduce((RowData a, RowData b) ->
                        a.getLong(CandleTableColumns.OUTPUT_TS) >= b.getLong(CandleTableColumns.OUTPUT_TS)
                                ? a : b)
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("migration-pass1-identical-hash-merge");

        // Pass 2: per (token, windowStart) candidate list + decision (bounded
        // sort-merge grouping — disk-backed, no CLI heap cap). BATCH mode has
        // no per-key aggregator: the keyed process function buffers candidates
        // in keyed list state and registers one processing-time timer per key;
        // the batch runtime drains every key's timers at key-group boundaries
        // (BatchExecutionInternalTimeService.setCurrentKey) and at end-of-input
        // (final Long.MAX_VALUE watermark → keySelected(null)). Main output is
        // the upsert row (CLEAN/APPROVED only, ROW_TYPE_INFO — no Kryo); the
        // decision metadata travels as a RowData-free POJO side output.
        SingleOutputStreamOperator<RowData> upsertRows = distinct
                .keyBy(new KeySelector<RowData, Tuple2<Long, Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> getKey(RowData row) {
                        return Tuple2.of(
                                row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                                row.getLong(CandleTableColumns.WINDOW_START));
                    }
                })
                .process(new KeyDecisionProcessFunction(cfg))
                .returns(CandleTableColumns.ROW_TYPE_INFO)
                .name("migration-pass2-keyed-aggregation");

        DataStream<KeyDecisionMeta> meta = upsertRows.getSideOutput(DECISION_OUT);
        DataStream<String> reports = meta.process(new ReportRenderer())
                .returns(Types.STRING)
                .name("migration-report-renderer");
        reports.addSink(new ReportFileSink(cfg.reportDir))
                .name("migration-report-file");

        meta.addSink(new MigrationGateSink(cfg))
                .name("migration-gate");

        // Row-count evidence (bounded global counts via constant-key reduce).
        DataStream<Tuple2<String, Long>> stats = countStats("total_rows", rows)
                .union(countStats("canonical_rows", canonical))
                .union(countStats("non_canonical_rows", nonCanonical));
        stats.addSink(new MigrationStatsSink()).name("migration-stats");

        if ("load".equals(cfg.mode)) {
            upsertRows.sinkTo(FlussSink.<RowData>builder()
                            .setBootstrapServers(cfg.bootstrap)
                            .setDatabase("default")
                            .setTable(cfg.destTable)
                            .setSerializationSchema(new RowDataSerializationSchema(false, false))
                            .build())
                    .name("migration-kv-upsert-sink");
        }
    }

    /** One-shot bounded count as a single {@code (kind, count)} element. */
    private static DataStream<Tuple2<String, Long>> countStats(String kind, DataStream<RowData> in) {
        return in.map(row -> 1L)
                .keyBy(value -> 0, Types.INT)
                .reduce(Long::sum)
                .map(count -> Tuple2.of(kind, count))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
    }

    // ── row identity: byte-identical to CandleMigrationTool.rowHash ──────────

    /**
     * Table-API source rows arrive as {@link org.apache.flink.types.Row};
     * converts to a {@link GenericRowData} in DDL order. The fixed 15-column
     * schema maps BIGINT→Long, INT→Integer, STRING→StringData — every accessor
     * used downstream ({@code getLong}/{@code getInt}/{@code getString}) is
     * satisfied by the generic row.
     */
    static RowData toRowData(org.apache.flink.types.Row row) {
        GenericRowData out = new GenericRowData(CandleTableColumns.FIELD_COUNT);
        for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
            Object value = row.getField(i);
            out.setField(i, value instanceof String ? StringData.fromString((String) value) : value);
        }
        return out;
    }

    /**
     * Deterministic business-field hash over a {@link RowData} candle —
     * SHA-256 over every column except {@code output_ts}, in DDL order,
     * encoded as {@code name=value\n}. Must produce the <em>same</em> digest
     * as {@link CandleMigrationTool#rowHash(org.apache.fluss.row.InternalRow)}
     * (the approval hashes were CLI-computed); pinned by
     * {@code CandleMigrationBatchJobTest.hashEquivalenceAcrossRowRepresentations}.
     */
    static String rowHash(RowData row) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
                if (i == CandleTableColumns.OUTPUT_TS) {
                    continue; // emit metadata, not row identity (CanonicalCandlePolicy)
                }
                md.update((CandleTableColumns.NAMES[i] + "=").getBytes(StandardCharsets.UTF_8));
                switch (i) {
                    case CandleTableColumns.TICK_COUNT:
                        md.update(Integer.toString(row.getInt(i)).getBytes(StandardCharsets.UTF_8));
                        break;
                    case CandleTableColumns.EXCHANGE:
                    case CandleTableColumns.SYMBOL:
                    case CandleTableColumns.ALGORITHM_VERSION:
                    case CandleTableColumns.CONFIGURATION_VERSION:
                    case CandleTableColumns.SCHEMA_VERSION:
                        md.update(row.getString(i).toString().getBytes(StandardCharsets.UTF_8));
                        break;
                    default:
                        md.update(Long.toString(row.getLong(i)).getBytes(StandardCharsets.UTF_8));
                }
                md.update((byte) '\n');
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16))
                        .append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("candle-migration: SHA-256 unavailable", e);
        }
    }

    /** Full business-field values, DDL order, {@code name=value} pairs (evidence). */
    static String businessValues(RowData row) {
        StringBuilder sb = new StringBuilder(192);
        for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
            if (i == CandleTableColumns.OUTPUT_TS) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(CandleTableColumns.NAMES[i]).append('=').append(businessValue(row, i));
        }
        return sb.toString();
    }

    private static String businessValue(RowData row, int index) {
        switch (index) {
            case CandleTableColumns.TICK_COUNT:
                return Integer.toString(row.getInt(index));
            case CandleTableColumns.EXCHANGE:
            case CandleTableColumns.SYMBOL:
            case CandleTableColumns.ALGORITHM_VERSION:
            case CandleTableColumns.CONFIGURATION_VERSION:
            case CandleTableColumns.SCHEMA_VERSION:
                return row.getString(index).toString();
            default:
                return Long.toString(row.getLong(index));
        }
    }

    // ── configuration ─────────────────────────────────────────────────────────

    /** Batch-job configuration, env names mirror {@code CandleMigrationTool.Config}. */
    static final class Config implements java.io.Serializable {
        final String bootstrap;
        final String sourceTable;
        final String destTable;
        final String schemaVersion;
        final String algorithmVersion;
        final String configurationVersion;
        final String acceptKeysFile;
        final String mode;
        final String reportDir;

        Config(String bootstrap, String sourceTable, String destTable, String schemaVersion,
               String algorithmVersion, String configurationVersion, String acceptKeysFile,
               String mode, String reportDir) {
            this.bootstrap = bootstrap;
            this.sourceTable = sourceTable;
            this.destTable = destTable;
            this.schemaVersion = schemaVersion;
            this.algorithmVersion = algorithmVersion;
            this.configurationVersion = configurationVersion;
            this.acceptKeysFile = acceptKeysFile;
            this.mode = mode;
            this.reportDir = reportDir;
        }

        static Config fromEnv() {
            String mode = System.getenv().getOrDefault("CANDLE_MIGRATION_MODE", "audit").trim();
            if (!"audit".equals(mode) && !"load".equals(mode)) {
                throw new IllegalArgumentException("candle-migration: CANDLE_MIGRATION_MODE must "
                        + "be 'audit' or 'load', got '" + mode + "' (tracker 14 P3.3)");
            }
            return new Config(
                    System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123"),
                    System.getenv().getOrDefault("CANDLE_TABLE", "feature_candles_15s"),
                    System.getenv().getOrDefault("CANDLE_CURRENT_TABLE", "feature_candles_15s_current"),
                    System.getenv().getOrDefault("SCHEMA_VERSION", "2"),
                    System.getenv().getOrDefault("ALGORITHM_VERSION",
                            com.trading.common.schema.CandleTableSchema.CANONICAL_ALGORITHM_VERSION),
                    System.getenv().getOrDefault("CONFIGURATION_VERSION",
                            com.trading.common.schema.CandleTableSchema.CANONICAL_CONFIGURATION_VERSION),
                    System.getenv("CANDLE_MIGRATION_ACCEPT_KEYS_FILE"),
                    mode,
                    System.getenv().getOrDefault("CANDLE_MIGRATION_REPORT_DIR",
                            "candle-migration-reports"));
        }
    }

    // ── decision records ──────────────────────────────────────────────────────

    /** POJO (public fields + no-arg ctor) so it travels as PojoTypeInfo. */
    public static final class KeyDecision {
        public long token;
        public long windowStart;
        public String decision; // CLEAN | APPROVED | UNACCEPTED | STALE
        public boolean truncated;
        /** Upsert row for CLEAN/APPROVED, null when the key is gated. */
        public RowData upsertRow;
        /** Rendered per-candidate evidence: {@code {hash=...,outputTs=...,...}}. */
        public List<String> candidateRecords;
        /** Plain business hashes, same order as candidateRecords (rejected-hash rendering). */
        public List<String> candidateHashes;
        public String acceptedHash;
        public String approver;
        public String reason;
        public String decidedAt;

        public KeyDecision() {}

        boolean conflict() {
            return !"CLEAN".equals(decision);
        }
    }

    /**
     * Decision metadata that crosses the network (gate + report rendering).
     * Deliberately RowData-free: every field serializes as a POJO without
     * Kryo, so the pass-2 shuffle never touches BinaryRowData internals.
     */
    public static final class KeyDecisionMeta {
        public long token;
        public long windowStart;
        public String decision; // CLEAN | APPROVED | UNACCEPTED | STALE
        public boolean truncated;
        /** Rendered per-candidate evidence: {@code {hash=...,outputTs=...,...}}. */
        public List<String> candidateRecords;
        /** Plain business hashes, same order as candidateRecords (rejected-hash rendering). */
        public List<String> candidateHashes;
        public String acceptedHash;
        public String approver;
        public String reason;
        public String decidedAt;

        public KeyDecisionMeta() {}

        static KeyDecisionMeta from(KeyDecision d) {
            KeyDecisionMeta m = new KeyDecisionMeta();
            m.token = d.token;
            m.windowStart = d.windowStart;
            m.decision = d.decision;
            m.truncated = d.truncated;
            m.candidateRecords = d.candidateRecords;
            m.candidateHashes = d.candidateHashes;
            m.acceptedHash = d.acceptedHash;
            m.approver = d.approver;
            m.reason = d.reason;
            m.decidedAt = d.decidedAt;
            return m;
        }

        String key() {
            return token + ":" + windowStart;
        }
    }

    /** Gate failure — job ends FAILED, mirroring the CLI's exit 2/exit 1. */
    static final class MigrationBlockedException extends RuntimeException {
        MigrationBlockedException(String message) {
            super(message);
        }
    }

    // ── operators ─────────────────────────────────────────────────────────────

    /** Canonical filter + null-key fail-closed (P3.4), mirroring {@code Audit.add}. */
    static final class CanonicalFilterProcessFunction extends ProcessFunction<RowData, RowData> {
        private final Config cfg;

        CanonicalFilterProcessFunction(Config cfg) {
            this.cfg = cfg;
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out) {
            String schemaVersion = row.getString(CandleTableColumns.SCHEMA_VERSION).toString();
            if (!cfg.schemaVersion.equals(schemaVersion)
                    || !CanonicalCandlePolicy.isCanonical(
                            row.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                            row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString(),
                            cfg.algorithmVersion, cfg.configurationVersion)) {
                ctx.output(NON_CANONICAL_OUT, row);
                return;
            }
            if (row.isNullAt(CandleTableColumns.INSTRUMENT_TOKEN)
                    || row.isNullAt(CandleTableColumns.WINDOW_START)) {
                throw new IllegalStateException("candle-migration: canonical row with null key "
                        + "column (instrument_token or window_start) — refusing to fabricate a key "
                        + "(tracker 14 P3.4)");
            }
            out.collect(row);
        }
    }

    /** Pass-2 accumulator: distinct business hashes (cap + truncated flag). */
    static final class CandAcc {
        final LinkedHashSet<String> hashes = new LinkedHashSet<>();
        final List<RowData> rows = new ArrayList<>();
        boolean truncated;
    }

    /**
     * Pass-2 per-key candidate grouping + approval resolution. BATCH mode:
     * buffers rows in keyed list state, registers one processing-time timer
     * per key on its first element; the batch runtime fires every key's timer
     * at the key-group boundary / end-of-input, where {@link #onTimer}
     * classifies the key, emits the upsert row (CLEAN/APPROVED) on the main
     * output and the decision metadata on {@link CandleMigrationBatchJob#DECISION_OUT}.
     * Parallelism is pinned to 1 (single-scan cutover), so no merge is needed.
     */
    static final class KeyDecisionProcessFunction
            extends KeyedProcessFunction<Tuple2<Long, Long>, RowData, RowData> {
        private final Config cfg;
        private transient ListState<RowData> candidates;
        private transient ValueState<Boolean> truncated;
        private Map<String, CandleMigrationTool.AcceptEntry> approvals = Map.of();

        KeyDecisionProcessFunction(Config cfg) {
            this.cfg = cfg;
        }

        @Override
        public void open(OpenContext openContext) {
            approvals = CandleMigrationTool.loadAcceptKeys(cfg.acceptKeysFile);
            candidates = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "migration-candidates", CandleTableColumns.ROW_TYPE_INFO));
            truncated = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("migration-truncated", Types.BOOLEAN));
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out)
                throws Exception {
            Iterable<RowData> existing = candidates.get();
            if (existing == null || !existing.iterator().hasNext()) {
                // First row of this key: arm the end-of-group timer. The batch
                // time service drains timers at the key boundary / end of input,
                // so one timer per key fires exactly once, after ALL its rows.
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime());
            }
            if (Boolean.TRUE.equals(truncated.value())) {
                return; // cap already reached; keep first 64 + truncated flag
            }
            int size = 0;
            for (RowData ignored : existing) {
                size++;
            }
            if (size >= MAX_CONFLICT_CANDIDATES) {
                truncated.update(true);
                return;
            }
            candidates.add(row);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
                throws Exception {
            CandAcc acc = new CandAcc();
            Iterable<RowData> existing = candidates.get();
            if (existing != null) {
                for (RowData row : existing) {
                    acc.rows.add(row);
                    acc.hashes.add(rowHash(row)); // unique per key (pass 1 dedup)
                }
            }
            acc.truncated = Boolean.TRUE.equals(truncated.value());
            KeyDecision d = classify(cfg, acc, approvals);
            if (d.upsertRow != null) {
                out.collect(d.upsertRow);
            }
            ctx.output(DECISION_OUT, KeyDecisionMeta.from(d));
            candidates.clear();
            truncated.clear();
        }
    }

    /** Pure classification — unit-testable without a cluster. */
    static KeyDecision classify(Config cfg, CandAcc acc,
                                Map<String, CandleMigrationTool.AcceptEntry> approvals) {
        KeyDecision d = new KeyDecision();
        RowData first = acc.rows.get(0);
        d.token = first.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
        d.windowStart = first.getLong(CandleTableColumns.WINDOW_START);
        d.truncated = acc.truncated;
        d.candidateRecords = renderCandidates(acc.rows);
        d.candidateHashes = new ArrayList<>(acc.hashes);

        boolean conflict = acc.rows.size() > 1 || acc.truncated;
        if (!conflict) {
            d.decision = "CLEAN";
            d.upsertRow = first; // pass 1 already kept the MAX(output_ts) row
            return d;
        }

        CandleMigrationTool.AcceptEntry accept = approvals.get(d.token + ":" + d.windowStart);
        if (accept == null) {
            d.decision = "UNACCEPTED";
            return d;
        }
        for (RowData candidate : acc.rows) {
            if (rowHash(candidate).equals(accept.rowHash())) {
                d.decision = "APPROVED";
                d.upsertRow = candidate;
                d.acceptedHash = accept.rowHash();
                d.approver = accept.approver();
                d.reason = accept.reason();
                d.decidedAt = accept.decidedAt();
                return d;
            }
        }
        d.decision = "STALE";
        d.acceptedHash = accept.rowHash();
        return d;
    }

    private static List<String> renderCandidates(List<RowData> rows) {
        List<String> records = new ArrayList<>(rows.size());
        for (RowData row : rows) {
            StringBuilder sb = new StringBuilder(256);
            sb.append("{hash=").append(rowHash(row))
                    .append(",outputTs=").append(row.getLong(CandleTableColumns.OUTPUT_TS))
                    .append(",algorithm=")
                    .append(row.getString(CandleTableColumns.ALGORITHM_VERSION))
                    .append(",configuration=")
                    .append(row.getString(CandleTableColumns.CONFIGURATION_VERSION))
                    .append(",values=").append(businessValues(row)).append("}");
            records.add(sb.toString());
        }
        return records;
    }

    /** Renders conflict + approval evidence records from decision metadata. */
    static final class ReportRenderer extends ProcessFunction<KeyDecisionMeta, String> {
        @Override
        public void processElement(KeyDecisionMeta d, Context ctx, Collector<String> out) {
            if (d.truncated || !"CLEAN".equals(d.decision)) {
                String approved = "STALE".equals(d.decision) ? "STALE"
                        : "UNACCEPTED".equals(d.decision) ? "MISSING" : "HASH_MATCH";
                StringBuilder sb = new StringBuilder(512);
                sb.append("CANDLE_MIGRATION_CONFLICT_RECORD=token=").append(d.token)
                        .append(",windowStart=").append(d.windowStart)
                        .append(",approved=").append(approved)
                        .append(",candidates=[");
                for (int i = 0; i < d.candidateRecords.size(); i++) {
                    if (i > 0) {
                        sb.append(';');
                    }
                    sb.append(d.candidateRecords.get(i));
                }
                sb.append(d.truncated ? ",truncated=true]" : "]");
                out.collect(sb.toString());
            }
            if ("APPROVED".equals(d.decision)) {
                StringBuilder sb = new StringBuilder(256);
                sb.append("CANDLE_MIGRATION_APPROVAL_RECORD=token=").append(d.token)
                        .append(",windowStart=").append(d.windowStart)
                        .append(",approvedHash=").append(d.acceptedHash)
                        .append(",rejectedHashes=");
                boolean first = true;
                for (String hash : d.candidateHashes) {
                    if (hash.equals(d.acceptedHash)) {
                        continue;
                    }
                    if (!first) {
                        sb.append(';');
                    }
                    sb.append(hash);
                    first = false;
                }
                if (d.approver != null) {
                    sb.append(",approver=").append(d.approver);
                }
                if (d.reason != null) {
                    sb.append(",reason=").append(d.reason);
                }
                if (d.decidedAt != null) {
                    sb.append(",decidedAt=").append(d.decidedAt);
                }
                out.collect(sb.toString());
            }
        }
    }

    /**
     * Gate: close() computes the summary and throws when any UNACCEPTED /
     * STALE / NOT_FOUND approval key exists (job FAILED = CLI exit 2 / exit 1).
     */
    static final class MigrationGateSink extends RichSinkFunction<KeyDecisionMeta> {
        private final Config cfg;
        private Map<String, CandleMigrationTool.AcceptEntry> approvals = Map.of();
        private final Set<String> seenKeys = new HashSet<>();
        private long conflicts;
        private long unaccepted;
        private long stale;

        MigrationGateSink(Config cfg) {
            this.cfg = cfg;
        }

        @Override
        public void open(OpenContext openContext) {
            approvals = CandleMigrationTool.loadAcceptKeys(cfg.acceptKeysFile);
        }

        @Override
        public void invoke(KeyDecisionMeta t, Context context) {
            seenKeys.add(t.key());
            if (t.truncated || !"CLEAN".equals(t.decision)) {
                conflicts++;
            }
            if ("UNACCEPTED".equals(t.decision)) {
                unaccepted++;
            }
            if ("STALE".equals(t.decision)) {
                stale++;
            }
        }

        @Override
        public void close() {
            long notFound = 0;
            for (String approvalKey : approvals.keySet()) {
                if (!seenKeys.contains(approvalKey)) {
                    notFound++;
                }
            }
            System.out.println("CANDLE_MIGRATION_DISTINCT_KEYS=" + seenKeys.size());
            System.out.println("CANDLE_MIGRATION_CONFLICTS=" + conflicts);
            System.out.println("CANDLE_MIGRATION_UNACCEPTED=" + unaccepted);
            System.out.println("CANDLE_MIGRATION_STALE=" + stale);
            System.out.println("CANDLE_MIGRATION_NOT_FOUND=" + notFound);
            if (!approvals.isEmpty()) {
                System.out.println("CANDLE_MIGRATION_DEV_EXCEPTION=1");
                System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS=" + approvals.size());
            }
            if (unaccepted > 0) {
                System.out.println("CANDLE_MIGRATION_STATUS=CONFLICT");
                LOG.error("candle-migration: {} conflicting keys not covered by an approval — "
                        + "aborting (B8.2/P3.1)", unaccepted);
                throw new MigrationBlockedException("candle-migration: " + unaccepted
                        + " unaccepted conflicting keys (tracker 14 P3.1) — job FAILED, "
                        + "mirroring CLI exit 2");
            }
            if (stale > 0 || notFound > 0) {
                System.out.println("CANDLE_MIGRATION_STATUS=ERROR");
                LOG.error("candle-migration: {} stale approvals and {} approval keys not found "
                        + "among canonical LOG keys — aborting (tracker 14 P3.1)", stale, notFound);
                throw new MigrationBlockedException("candle-migration: " + stale
                        + " stale approvals, " + notFound
                        + " approvals not found — job FAILED, mirroring CLI exit 1");
            }
            System.out.println("CANDLE_MIGRATION_STATUS=OK");
        }
    }

    /**
     * Writes the conflict/approval evidence records to a single deterministic
     * file under the report dir (parallelism 1 — one writer, one file).
     */
    static final class ReportFileSink extends RichSinkFunction<String> {
        private final String dirPath;
        private transient java.io.BufferedWriter writer;

        ReportFileSink(String dirPath) {
            this.dirPath = dirPath;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            Files.createDirectories(Path.of(dirPath));
            writer = Files.newBufferedWriter(
                    Path.of(dirPath, "conflict-and-approval-records"), StandardCharsets.UTF_8);
        }

        @Override
        public void invoke(String line, Context context) throws Exception {
            writer.write(line);
            writer.newLine();
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    /** Prints bounded row-count evidence ({@code CANDLE_MIGRATION_<KIND>=<n>}). */
    static final class MigrationStatsSink extends RichSinkFunction<Tuple2<String, Long>> {
        @Override
        public void invoke(Tuple2<String, Long> stat, Context context) {
            System.out.println("CANDLE_MIGRATION_" + stat.f0.toUpperCase() + "=" + stat.f1);
        }
    }
}
