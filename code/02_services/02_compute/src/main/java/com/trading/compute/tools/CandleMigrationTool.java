package com.trading.compute.tools;

import com.trading.compute.signaljob.CandleTableColumns;
import com.trading.compute.signaljob.CanonicalCandlePolicy;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline LOG→KV migration tool for the candle current-state projection
 * (CANDLE-KV-REPLAY-001 B8.2/B8.3).
 *
 * <p>{@code audit} reads the whole source LOG ({@code feature_candles_15s})
 * and reports, per B8.2: total rows, canonical rows (schema_version +
 * approved algorithm/configuration), non-canonical rows, distinct canonical
 * keys, duplicate keys, and conflicting business values. Business conflict
 * fields are every column <em>except</em> {@code output_ts} — two rows for the
 * same key that agree on the business fields are the same canonical candle
 * re-emitted (replay), while disagreeing business values mean the LOG holds
 * genuinely different candles for one key (B8.2 aborts on that). The scan is
 * read-only; the LOG is never written.
 *
 * <p>{@code load} re-runs the audit, aborts if any conflict exists, then
 * upserts exactly one row per canonical key into the destination KV table —
 * the row with {@code MAX(output_ts)} (B8.3: legal because all business
 * values are equal) — and verifies the destination row count afterwards.
 *
 * <p>Environment (same defaults as the SignalJob): {@code
 * FLUSS_BOOTSTRAP_SERVERS} (default {@code localhost:9123}), {@code
 * CANDLE_TABLE} (default {@code feature_candles_15s}), {@code
 * CANDLE_CURRENT_TABLE} (default {@code feature_candles_15s_current}), {@code
 * SCHEMA_VERSION} (default {@code 2}), {@code ALGORITHM_VERSION} (default
 * {@code candle-15s-v1}), {@code CONFIGURATION_VERSION} (default {@code
 * 1.0.0}), and {@code CANDLE_MIGRATION_ACCEPT_KEYS_FILE} (optional: a UTF-8
 * file with one {@code token,windowStart} per line; blank lines and {@code #}
 * comments allowed). An accept-list entry records the recorded data-ops
 * decision that a specific conflicting key may merge by {@code MAX(output_ts)}
 * even though its business values differ (the 2026-08-10 replay-incident
 * window). Conflicts NOT on the list still abort; accept-list entries that
 * match no canonical LOG key abort as errors (typo/stale-list detection).
 *
 * <p>Usage: {@code java -cp <compute classes>:<classpath>
 * com.trading.compute.tools.CandleMigrationTool audit|load}
 *
 * <p>Exit codes: 0 = OK (all conflicts covered by the accept list count as
 * accepted), 2 = business conflicts not covered by the accept list (abort per
 * B8.2), 1 = error (I/O, bad configuration, malformed accept file, or
 * accept-list entries not found in the canonical LOG).
 * Machine-readable stdout lines are prefixed {@code CANDLE_MIGRATION_}.
 *
 * <p>Scope note (dev run): the local dev Fluss cluster has the datalake tier
 * disabled, so the complete LOG history is read through the Fluss scan API
 * (the same data plane the sink writes). The production runbook keeps the
 * Flink/Fluss catalog union-read across Iceberg-tiered data as an operator
 * precondition (B8.1) before this tool is approved to run there.
 */
public final class CandleMigrationTool {

    private static final Logger LOG = LoggerFactory.getLogger(CandleMigrationTool.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final int SCAN_LIMIT = 1_000_000_000;

    private CandleMigrationTool() {}

    /** Tool configuration, resolved from the environment with SignalJob defaults. */
    static final class Config {
        final String bootstrap;
        final String sourceTable;
        final String destTable;
        final String schemaVersion;
        final String algorithmVersion;
        final String configurationVersion;
        final String acceptKeysFile;

        Config(String bootstrap, String sourceTable, String destTable,
               String schemaVersion, String algorithmVersion, String configurationVersion,
               String acceptKeysFile) {
            this.bootstrap = bootstrap;
            this.sourceTable = sourceTable;
            this.destTable = destTable;
            this.schemaVersion = schemaVersion;
            this.algorithmVersion = algorithmVersion;
            this.configurationVersion = configurationVersion;
            this.acceptKeysFile = acceptKeysFile;
        }

        static Config fromEnv() {
            return new Config(
                    System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123"),
                    System.getenv().getOrDefault("CANDLE_TABLE", "feature_candles_15s"),
                    System.getenv().getOrDefault("CANDLE_CURRENT_TABLE", "feature_candles_15s_current"),
                    System.getenv().getOrDefault("SCHEMA_VERSION", "2"),
                    System.getenv().getOrDefault("ALGORITHM_VERSION", "candle-15s-v1"),
                    System.getenv().getOrDefault("CONFIGURATION_VERSION", "1.0.0"),
                    System.getenv("CANDLE_MIGRATION_ACCEPT_KEYS_FILE"));
        }
    }

    /**
     * Pure audit accumulator (B8.2): canonical filter, per-key grouping,
     * business-conflict detection, MAX(output_ts) merge target. No I/O — the
     * unit tests exercise this directly.
     */
    static final class Audit {
        private final String expectedSchemaVersion;
        private final String expectedAlgorithm;
        private final String expectedConfiguration;
        private final Set<String> acceptedKeys;

        long totalRows;
        long canonicalRows;
        long nonCanonicalRows;
        long duplicateKeys;
        long conflictingKeys;
        /** Conflicts covered by the accept list (recorded data-ops decision). */
        long acceptedKeysCount;
        /** Conflicts NOT covered by the accept list — the abort gate. */
        long unacceptedConflictingKeys;
        final List<String> conflictExamples = new ArrayList<>();
        /** token -> windowStart -> aggregate (two-level map: no packed-key collisions). */
        final Map<Long, Map<Long, KeyAgg>> byKey = new HashMap<>();

        Audit(String expectedSchemaVersion, String expectedAlgorithm, String expectedConfiguration) {
            this(expectedSchemaVersion, expectedAlgorithm, expectedConfiguration, Set.of());
        }

        Audit(String expectedSchemaVersion, String expectedAlgorithm, String expectedConfiguration,
              Set<String> acceptedKeys) {
            this.expectedSchemaVersion = expectedSchemaVersion;
            this.expectedAlgorithm = expectedAlgorithm;
            this.expectedConfiguration = expectedConfiguration;
            this.acceptedKeys = acceptedKeys == null ? Set.of() : acceptedKeys;
        }

        /** Feed one source row; filters and aggregates it. */
        void add(InternalRow row) {
            totalRows++;
            if (!isCanonical(row)) {
                nonCanonicalRows++;
                return;
            }
            canonicalRows++;
            long token = row.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
            long windowStart = row.getLong(CandleTableColumns.WINDOW_START);
            boolean accepted = acceptedKeys.contains(key(token, windowStart));
            Map<Long, KeyAgg> windows = byKey.computeIfAbsent(token, k -> new HashMap<>());
            KeyAgg agg = windows.computeIfAbsent(windowStart, k -> new KeyAgg(accepted));
            agg.add(row);
            if (agg.rows == 2) {
                duplicateKeys++;
            }
            if (agg.conflict && !agg.conflictCounted) {
                agg.conflictCounted = true;
                conflictingKeys++;
                if (agg.accepted) {
                    acceptedKeysCount++;
                } else {
                    unacceptedConflictingKeys++;
                    if (conflictExamples.size() < 5) {
                        conflictExamples.add("token=" + token + " windowStart=" + windowStart
                                + " " + agg.conflictField + " differs (first=" + agg.firstValue
                                + " vs later=" + agg.laterValue + ")");
                    }
                }
            }
        }

        private boolean isCanonical(InternalRow row) {
            if (!expectedSchemaVersion.equals(
                    row.getString(CandleTableColumns.SCHEMA_VERSION).toString())) {
                return false;
            }
            return CanonicalCandlePolicy.isCanonical(
                    row.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                    row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString(),
                    expectedAlgorithm, expectedConfiguration);
        }

        long distinctKeys() {
            long keys = 0;
            for (Map<Long, KeyAgg> windows : byKey.values()) {
                keys += windows.size();
            }
            return keys;
        }

        /**
         * Accept-list entries that match no canonical LOG key — typo or stale
         * list detection (fail-closed: the operator intended an override for a
         * key the source does not actually hold).
         */
        long acceptedKeysNotFound() {
            if (acceptedKeys.isEmpty()) {
                return 0;
            }
            Set<String> seen = new HashSet<>();
            for (Map.Entry<Long, Map<Long, KeyAgg>> entry : byKey.entrySet()) {
                for (Long windowStart : entry.getValue().keySet()) {
                    seen.add(key(entry.getKey(), windowStart));
                }
            }
            long notFound = 0;
            for (String k : acceptedKeys) {
                if (!seen.contains(k)) {
                    notFound++;
                }
            }
            return notFound;
        }

        private static String key(long token, long windowStart) {
            return token + ":" + windowStart;
        }
    }

    /** Per-key aggregate: row count, conflict detection, MAX(output_ts) row. */
    static final class KeyAgg {
        /** Recorded operator decision: merge this key by MAX(output_ts) even on conflict. */
        final boolean accepted;
        long rows;
        boolean conflict;
        boolean conflictCounted;
        String conflictField;
        String firstValue;
        String laterValue;
        InternalRow rowAtMaxOutputTs;
        long maxOutputTs = Long.MIN_VALUE;

        KeyAgg() {
            this(false);
        }

        KeyAgg(boolean accepted) {
            this.accepted = accepted;
        }

        void add(InternalRow row) {
            if (rows == 0) {
                rowAtMaxOutputTs = row;
                maxOutputTs = row.getLong(CandleTableColumns.OUTPUT_TS);
            } else {
                if (!conflict) {
                    Integer index = businessConflictIndex(rowAtMaxOutputTs, row);
                    if (index != null) {
                        conflict = true;
                        conflictField = CandleTableColumns.NAMES[index];
                        firstValue = businessValue(rowAtMaxOutputTs, index);
                        laterValue = businessValue(row, index);
                    }
                }
                if (row.getLong(CandleTableColumns.OUTPUT_TS) > maxOutputTs) {
                    maxOutputTs = row.getLong(CandleTableColumns.OUTPUT_TS);
                    rowAtMaxOutputTs = row;
                }
            }
            rows++;
        }

        /** @return the index of the first differing business field, or null if equal. */
        private static Integer businessConflictIndex(InternalRow a, InternalRow b) {
            for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
                if (i == CandleTableColumns.OUTPUT_TS) {
                    continue; // emit metadata, not row identity (CanonicalCandlePolicy)
                }
                if (!businessValue(a, i).equals(businessValue(b, i))) {
                    return i;
                }
            }
            return null;
        }

        private static String businessValue(InternalRow row, int index) {
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
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : null;
        if (!"audit".equals(mode) && !"load".equals(mode)) {
            System.err.println("usage: CandleMigrationTool audit|load");
            System.err.println("  audit  — dry-run B8.2: count/filter/report, abort on conflicts");
            System.err.println("  load   — B8.3: audit, then upsert one row per canonical key into the KV table");
            System.exit(1);
            return;
        }

        Config cfg = Config.fromEnv();
        Set<String> acceptedKeys = loadAcceptKeys(cfg.acceptKeysFile);
        try (Connection connection = connect(cfg)) {
            Admin admin = connection.getAdmin();
            TableInfo sourceInfo = tableInfo(admin, cfg.sourceTable, "source", cfg.bootstrap);
            tableInfo(admin, cfg.destTable, "destination", cfg.bootstrap);

            LOG.info("candle-migration: mode={} source={} dest={} filter(schema_version={}, "
                            + "algorithm_version={}, configuration_version={}) acceptKeysFile={}",
                    mode, cfg.sourceTable, cfg.destTable, cfg.schemaVersion,
                    cfg.algorithmVersion, cfg.configurationVersion, cfg.acceptKeysFile);

            Audit audit = scanSource(connection, sourceInfo, cfg, acceptedKeys);
            printAudit(audit, cfg, mode);
            long acceptedKeysNotFound = audit.acceptedKeysNotFound();

            if (audit.unacceptedConflictingKeys > 0) {
                System.out.println("CANDLE_MIGRATION_STATUS=CONFLICT");
                LOG.error("candle-migration: {} conflicting business values not covered by the "
                                + "accept list — aborting (B8.2/B8.3)",
                        audit.unacceptedConflictingKeys);
                System.exit(2);
                return;
            }
            if (acceptedKeysNotFound > 0) {
                System.out.println("CANDLE_MIGRATION_STATUS=ERROR");
                LOG.error("candle-migration: {} accept-list keys not found among canonical LOG "
                                + "keys — aborting (typo or stale list)",
                        acceptedKeysNotFound);
                System.exit(1);
                return;
            }
            if (audit.acceptedKeysCount > 0) {
                LOG.warn("candle-migration: {} conflicting keys accepted via {} — "
                                + "MAX(output_ts) row wins for those keys (recorded decision)",
                        audit.acceptedKeysCount, cfg.acceptKeysFile);
            }

            if ("audit".equals(mode)) {
                System.out.println("CANDLE_MIGRATION_STATUS=OK");
                return;
            }

            long loaded = load(connection, cfg, audit);
            long destRows = countRows(connection, cfg.destTable, cfg.bootstrap);
            System.out.println("CANDLE_MIGRATION_LOADED=" + loaded);
            System.out.println("CANDLE_MIGRATION_DEST_ROWS_AFTER=" + destRows);
            System.out.println("CANDLE_MIGRATION_STATUS=OK");
            LOG.info("candle-migration: load complete — {} canonical keys loaded, "
                            + "destination {} now holds {} rows",
                    loaded, cfg.destTable, destRows);
        }
    }

    // ── scan / aggregate ───────────────────────────────────────────────────

    private static Audit scanSource(Connection connection, TableInfo sourceInfo, Config cfg,
                                    Set<String> acceptedKeys)
            throws Exception {
        Audit audit = new Audit(cfg.schemaVersion, cfg.algorithmVersion, cfg.configurationVersion,
                acceptedKeys);
        Table source = connection.getTable(TablePath.of("default", cfg.sourceTable));
        for (int b = 0; b < sourceInfo.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(sourceInfo.getTableId(), b);
            try (BatchScanner scanner = source.newScan().limit(SCAN_LIMIT).createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(30_000))) {
                while (it.hasNext()) {
                    audit.add(it.next());
                }
            }
        }
        LOG.info("candle-migration: scan complete — total={} canonical={} nonCanonical={} "
                        + "distinctKeys={} duplicateKeys={} conflictingKeys={}",
                audit.totalRows, audit.canonicalRows, audit.nonCanonicalRows,
                audit.distinctKeys(), audit.duplicateKeys, audit.conflictingKeys);
        return audit;
    }

    private static void printAudit(Audit audit, Config cfg, String mode) {
        System.out.println("CANDLE_MIGRATION_MODE=" + mode);
        System.out.println("CANDLE_MIGRATION_SOURCE=" + cfg.sourceTable);
        System.out.println("CANDLE_MIGRATION_FILTER=schema_version=" + cfg.schemaVersion
                + ",algorithm_version=" + cfg.algorithmVersion
                + ",configuration_version=" + cfg.configurationVersion);
        System.out.println("CANDLE_MIGRATION_TOTAL_ROWS=" + audit.totalRows);
        System.out.println("CANDLE_MIGRATION_CANONICAL_ROWS=" + audit.canonicalRows);
        System.out.println("CANDLE_MIGRATION_NON_CANONICAL_ROWS=" + audit.nonCanonicalRows);
        System.out.println("CANDLE_MIGRATION_DISTINCT_KEYS=" + audit.distinctKeys());
        System.out.println("CANDLE_MIGRATION_DUPLICATE_KEYS=" + audit.duplicateKeys);
        System.out.println("CANDLE_MIGRATION_CONFLICTING_KEYS=" + audit.conflictingKeys);
        System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS_FILE="
                + (cfg.acceptKeysFile == null ? "" : cfg.acceptKeysFile));
        System.out.println("CANDLE_MIGRATION_ACCEPTED_KEYS=" + audit.acceptedKeysCount);
        System.out.println("CANDLE_MIGRATION_UNACCEPTED_KEYS=" + audit.unacceptedConflictingKeys);
        System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS_NOT_FOUND=" + audit.acceptedKeysNotFound());
        for (String example : audit.conflictExamples) {
            System.out.println("CANDLE_MIGRATION_CONFLICT_EXAMPLE=" + example);
        }
    }

    /**
     * Loads the operator-approved accept list ({@code token,windowStart} per
     * line; blank lines and {@code #} comments allowed). A missing/blank env
     * value yields an empty set (pre-decision behavior); an unreadable file or
     * a malformed line fails closed with {@link IllegalArgumentException}.
     */
    static Set<String> loadAcceptKeys(String path) {
        Set<String> keys = new HashSet<>();
        if (path == null || path.isBlank()) {
            return keys;
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(Path.of(path), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "candle-migration: cannot read accept-keys file " + path, e);
        }
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split(",", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("candle-migration: malformed accept-keys line "
                        + (i + 1) + " in " + path + ": \"" + line
                        + "\" (expected token,windowStart)");
            }
            try {
                long token = Long.parseLong(parts[0].trim());
                long windowStart = Long.parseLong(parts[1].trim());
                keys.add(token + ":" + windowStart);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("candle-migration: malformed accept-keys line "
                        + (i + 1) + " in " + path + ": \"" + line
                        + "\" (token and windowStart must be integers)", e);
            }
        }
        return keys;
    }

    // ── load ───────────────────────────────────────────────────────────────

    private static long load(Connection connection, Config cfg, Audit audit) throws Exception {
        Table dest = connection.getTable(TablePath.of("default", cfg.destTable));
        UpsertWriter writer = dest.newUpsert().createWriter();
        long loaded = 0;
        try {
            for (Map<Long, KeyAgg> windows : audit.byKey.values()) {
                for (KeyAgg agg : windows.values()) {
                    writer.upsert(copyRow(agg.rowAtMaxOutputTs))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    loaded++;
                }
            }
        } finally {
            writer.flush();
        }
        return loaded;
    }

    /** Rebuild a plain GenericRow from the scan row (typed getters — safe for
     *  GenericRow and Arrow-backed scan rows alike). */
    private static GenericRow copyRow(InternalRow row) {
        return GenericRow.of(
                row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                BinaryString.fromString(row.getString(CandleTableColumns.EXCHANGE).toString()),
                BinaryString.fromString(row.getString(CandleTableColumns.SYMBOL).toString()),
                row.getLong(CandleTableColumns.WINDOW_START),
                row.getLong(CandleTableColumns.WINDOW_END),
                row.getLong(CandleTableColumns.OPEN_PAISE),
                row.getLong(CandleTableColumns.HIGH_PAISE),
                row.getLong(CandleTableColumns.LOW_PAISE),
                row.getLong(CandleTableColumns.CLOSE_PAISE),
                row.getLong(CandleTableColumns.VOLUME),
                row.getInt(CandleTableColumns.TICK_COUNT),
                BinaryString.fromString(row.getString(CandleTableColumns.ALGORITHM_VERSION).toString()),
                BinaryString.fromString(row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString()),
                row.getLong(CandleTableColumns.OUTPUT_TS),
                BinaryString.fromString(row.getString(CandleTableColumns.SCHEMA_VERSION).toString()));
    }

    private static long countRows(Connection connection, String tableName, String bootstrap)
            throws Exception {
        TableInfo info = tableInfo(connection.getAdmin(), tableName, "count", bootstrap);
        Table table = connection.getTable(TablePath.of("default", tableName));
        long count = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan().limit(SCAN_LIMIT).createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(30_000))) {
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
            }
        }
        return count;
    }

    // ── plumbing ───────────────────────────────────────────────────────────

    private static Connection connect(Config cfg) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", cfg.bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        LOG.info("candle-migration: connected to {}", cfg.bootstrap);
        return connection;
    }

    private static TableInfo tableInfo(Admin admin, String tableName, String role, String bootstrap)
            throws Exception {
        try {
            TableInfo info = admin.getTableInfo(TablePath.of("default", tableName))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            LOG.info("candle-migration: {} table {} (id={}, buckets={}, pk={}, bucketKeys={})",
                    role, tableName, info.getTableId(), info.getNumBuckets(),
                    info.getPrimaryKeys(), info.getBucketKeys());
            return info;
        } catch (Exception e) {
            throw new IllegalStateException("candle-migration: " + role + " table "
                    + tableName + " not available at " + bootstrap, e);
        }
    }
}
