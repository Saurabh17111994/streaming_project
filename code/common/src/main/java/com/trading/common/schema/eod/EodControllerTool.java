package com.trading.common.schema.eod;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.CloseableIterator;

/**
 * EOD controller CLI (SCH-23): the thin runner over {@link EodPlanner} /
 * {@link EodController} against live Fluss tables, mirroring the ddl-apply
 * tool pattern (one-shot subcommands, {@code FLUSS_BOOTSTRAP} via env,
 * machine-readable {@code eod-controller: RESULT=... EXIT=...} sentinel).
 *
 * <pre>{@code
 * eod-controller status    read-only per-table plan + day-state summary
 * eod-controller run       advance due days through the offload state machine
 *                          (creates the run-date PENDING record per table)
 * eod-controller extend    retention-extension recipe; --apply performs the
 *                          shadow-table rewrite drill
 * eod-controller reconcile re-verify COMMITTED/VERIFYING days (crash-resume)
 * eod-controller reset     FAILED_MANUAL -> PENDING (requires --approve)
 * }</pre>
 *
 * <p>Exit codes: 0 OK (or extension applied / days verified), 1 failure,
 * 2 extension required (or retryable days), 3 pending work (status),
 * 4 usage/approval-required, 5 lease held by another controller.
 *
 * <p>The retention-extension mechanism honors the Fluss 0.9.1 boundary:
 * {@code table.log.ttl} is create-time only (verified 2026-08-13), so an
 * extension is a controlled rewrite — {@code extend --apply} creates a shadow
 * table with the extended create-time TTL ({@code name__eod_ext_<date>}),
 * copies the current rows, and verifies count parity. The swap (pointing
 * consumers at the shadow, or drop+recreate under the same name) stays an
 * operator-approved step — the drill measures the copy before production
 * assumptions harden.
 */
public final class EodControllerTool {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** The ten 2d-TTL live tables (2026-08-13 recreation evidence) — the documented default scope. */
    static final List<String> DEFAULT_TABLES = List.of(
            "raw_table_1", "feature_candles_15s", "ingestion_quarantine",
            "Order_Lifecycle", "suspected_discontinuities", "Postback_Quarantine",
            "Trade_Decisions", "Ranking_Results", "Portfolio_Reservations",
            "Postback_Projection_Ledger");

    private EodControllerTool() {}

    public static void main(String[] args) {
        try {
            System.exit(run(args));
        } catch (Throwable t) {
            System.err.println("eod-controller: FATAL — " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    static int run(String[] args) throws Exception {
        for (String a : args) {
            if (a.equals("--help") || a.equals("-h")) {
                return usage();
            }
        }
        Options opts = Options.parse(args);
        ZoneId zone = ZoneId.of(opts.zone);
        Instant now = Instant.now();
        LocalDate runDate = opts.runDate != null ? LocalDate.parse(opts.runDate)
                : LocalDate.now(zone);
        long nowMs = now.toEpochMilli();

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", opts.bootstrap);
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            // Live TTLs — the controller plans against each table's ACTUAL
            // create-time TTL, with a documented fallback when metadata lacks it.
            Map<String, Duration> liveTtls = new LinkedHashMap<>();
            for (String table : opts.tables) {
                liveTtls.put(table, liveTtl(admin, opts.database, table, opts.ttlDefault));
            }

            return switch (opts.subcommand) {
                case "status" -> status(opts, liveTtls, zone, now);
                case "run" -> run(opts, liveTtls, zone, runDate, now, nowMs);
                case "extend" -> extend(opts, connection, admin, liveTtls, zone, now);
                case "reconcile" -> reconcile(opts, liveTtls, now);
                case "reset" -> reset(opts, now);
                default -> usage();
            };
        }
    }

    // ── subcommands ───────────────────────────────────────────────────────

    private static int status(Options opts, Map<String, Duration> liveTtls, ZoneId zone,
            Instant now) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            List<EodOffloadRecord> days = store.readAll();
            List<EodController.TablePlan> plans = EodController.planTables(
                    days, opts.tables, liveTtls, zone, opts.safetyFloor, now);
            for (EodController.TablePlan p : plans) {
                if (p.noDays()) {
                    System.out.println("eod-controller: status " + p.table()
                            + " — no days on file");
                    continue;
                }
                System.out.println("eod-controller: status " + p.table()
                        + " earliestUnverified=" + p.plan().earliestUnverifiedDate()
                        + " protectedBound=" + p.plan().protectedExpiryBound()
                        + " margin=" + p.plan().marginMs() + "ms"
                        + " requiresExtension=" + p.plan().requiresExtension()
                        + " verified=" + p.verifiedDays() + " unverified=" + p.unverifiedDays());
                for (EodOffloadRecord d : p.days()) {
                    System.out.println("eod-controller:   day " + d.tradingDate()
                            + " state=" + d.state() + " retry=" + d.retryCount()
                            + " nextRetry=" + (d.nextRetryAtMs() == 0 ? "-"
                            : Instant.ofEpochMilli(d.nextRetryAtMs())));
                }
            }
            EodController.Status s = EodController.statusOf(plans, opts.safetyFloor);
            String result = switch (s) {
                case OK -> "OK";
                case EXTENSION_REQUIRED -> "EXTENSION_REQUIRED";
                case PENDING_WORK -> "PENDING_WORK";
            };
            int exit = switch (s) {
                case OK -> 0;
                case EXTENSION_REQUIRED -> 2;
                case PENDING_WORK -> 3;
            };
            System.out.println("eod-controller: RESULT=" + result + " EXIT=" + exit
                    + " TABLES=" + opts.tables.size() + " DAYS=" + days.size());
            return exit;
        }
    }

    private static int run(Options opts, Map<String, Duration> liveTtls, ZoneId zone,
            LocalDate runDate, Instant now, long nowMs) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            if (opts.dryRun) {
                List<EodOffloadRecord> days = store.readAll();
                List<EodOffloadRecord> due = EodController.dueDays(days, runDate, nowMs);
                System.out.println("eod-controller: dry-run — run-date " + runDate
                        + " dueDays=" + due.size() + " executor=" + opts.offloadMode);
                for (EodOffloadRecord d : due) {
                    System.out.println("eod-controller:   would advance " + d.tableName()
                            + " " + d.tradingDate() + " (" + d.state() + ")");
                }
                System.out.println("eod-controller: RESULT=DRY_RUN EXIT=0 TABLES="
                        + opts.tables.size() + " DAYS=" + due.size());
                return 0;
            }
            String myToken = token();
            Lease lease = store.acquireLease(myToken, nowMs, opts.leaseTtl.toMillis());
            if (!lease.isHeldBy(myToken, nowMs)) {
                System.err.println("eod-controller: lease held by " + lease.token()
                        + " until " + Instant.ofEpochMilli(lease.expiryMs())
                        + " — refusing to run (single-writer fencing)");
                System.out.println("eod-controller: RESULT=LEASED EXIT=5 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 5;
            }
            EodOffloadExecutor executor = opts.offloadMode.equalsIgnoreCase("mock")
                    ? new MockEodOffloadExecutor(true, true)
                    : NotConfiguredEodOffloadExecutor.INSTANCE;
            List<EodController.RunOutcome> outcomes = EodController.runOnce(store, executor,
                    runDate, opts.tables, opts.schemaVersion, now);
            return reportRunOutcomes(outcomes, "run");
        }
    }

    private static int extend(Options opts, Connection connection, Admin admin,
            Map<String, Duration> liveTtls, ZoneId zone, Instant now) throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            List<EodOffloadRecord> days = store.readAll();
            List<EodController.TablePlan> plans = EodController.planTables(
                    days, opts.tables, liveTtls, zone, opts.safetyFloor, now);
            boolean required = false;
            boolean appliedAll = true;
            for (EodController.TablePlan p : plans) {
                if (p.noDays() || !p.plan().requiresExtension()) {
                    continue;
                }
                required = true;
                Duration live = liveTtls.get(p.table());
                Duration newTtl = EodRetentionPolicy.extendedTtl(live, opts.extension);
                String shadow = p.table() + "__eod_ext_"
                        + now.atZone(zone).format(DateTimeFormatter.BASIC_ISO_DATE);
                System.out.println("eod-controller: extend " + p.table()
                        + " liveTtl=" + live + " newTtl=" + newTtl
                        + " shadow=" + shadow + " margin=" + p.plan().marginMs() + "ms");
                if (opts.apply && !opts.dryRun) {
                    if (!performRewrite(connection, admin, opts.database, p.table(),
                            shadow, newTtl, TIMEOUT.toMillis())) {
                        appliedAll = false;
                    }
                }
            }
            if (!required) {
                System.out.println("eod-controller: RESULT=OK EXIT=0 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 0;
            }
            if (opts.dryRun) {
                // recipe only — the drill is not executed
                System.out.println("eod-controller: RESULT=EXTENSION_REQUIRED EXIT=2 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 2;
            }
            if (opts.apply) {
                if (appliedAll) {
                    System.out.println("eod-controller: RESULT=EXTENDED EXIT=0 TABLES="
                            + opts.tables.size() + " DAYS=0");
                    return 0;
                }
                System.err.println("eod-controller: RESULT=EXTEND_FAILED EXIT=1 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 1;
            }
            System.out.println("eod-controller: RESULT=EXTENSION_REQUIRED EXIT=2 TABLES="
                    + opts.tables.size() + " DAYS=0");
            return 2;
        }
    }

    private static int reconcile(Options opts, Map<String, Duration> liveTtls, Instant now)
            throws Exception {
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            String myToken = token();
            Lease lease = store.acquireLease(myToken, now.toEpochMilli(),
                    opts.leaseTtl.toMillis());
            if (!lease.isHeldBy(myToken, now.toEpochMilli())) {
                System.err.println("eod-controller: lease held by " + lease.token()
                        + " — refusing to reconcile");
                System.out.println("eod-controller: RESULT=LEASED EXIT=5 TABLES="
                        + opts.tables.size() + " DAYS=0");
                return 5;
            }
            EodOffloadExecutor executor = opts.offloadMode.equalsIgnoreCase("mock")
                    ? new MockEodOffloadExecutor(true, true)
                    : NotConfiguredEodOffloadExecutor.INSTANCE;
            List<EodController.RunOutcome> outcomes = EodController.reconcile(
                    store, executor, opts.tables, now);
            return reportRunOutcomes(outcomes, "reconcile");
        }
    }

    private static int reset(Options opts, Instant now) throws Exception {
        if (!opts.approve) {
            System.err.println("eod-controller: reset is destructive — pass --approve "
                    + "(FAILED_MANUAL -> PENDING)");
            System.out.println("eod-controller: RESULT=APPROVAL_REQUIRED EXIT=4 TABLES="
                    + opts.tables.size() + " DAYS=0");
            return 4;
        }
        try (FlussEodStateStore store = FlussEodStateStore.open(opts.bootstrap, opts.database,
                opts.stateTable, TIMEOUT)) {
            int reset = EodController.resetManual(store, now, opts.runDate, opts.singleTable);
            System.out.println("eod-controller: reset " + reset
                    + " FAILED_MANUAL day(s) -> PENDING");
            System.out.println("eod-controller: RESULT=RESET EXIT=0 TABLES="
                    + opts.tables.size() + " DAYS=" + reset);
            return 0;
        }
    }

    private static int reportRunOutcomes(List<EodController.RunOutcome> outcomes, String phase) {
        int verified = 0, retryable = 0;
        Set<String> tables = outcomes.stream()
                .map(EodController.RunOutcome::table).collect(Collectors.toSet());
        for (EodController.RunOutcome o : outcomes) {
            System.out.println("eod-controller: " + phase + " " + o.table() + " "
                    + o.tradingDate() + " " + o.from() + " -> " + o.to()
                    + (o.verified() ? " VERIFIED" : "")
                    + (o.note().isEmpty() ? "" : " (" + o.note() + ")"));
            if (o.verified()) {
                verified++;
            } else {
                retryable++;
            }
        }
        int exit = retryable > 0 ? 2 : 0;
        String result = retryable > 0 ? "RETRYABLE" : "VERIFIED";
        System.out.println("eod-controller: RESULT=" + result + " EXIT=" + exit
                + " TABLES=" + tables.size() + " DAYS=" + outcomes.size());
        return exit;
    }

    // ── extend rewrite drill (live) ───────────────────────────────────────

    /**
     * The controlled-rewrite drill: create the shadow table with the extended
     * create-time TTL (same schema/PK/distribution, lake disabled like the
     * live dev tables), copy every current row via the raw client, and verify
     * count parity. The swap stays an operator step — 0.9.1 has no rename.
     * Returns true when the shadow was created and the copy reconciles.
     */
    static boolean performRewrite(Connection connection, Admin admin, String database,
            String table, String shadow, Duration newTtl, long timeoutMs) throws Exception {
        TablePath livePath = TablePath.of(database, table);
        TablePath shadowPath = TablePath.of(database, shadow);
        TableInfo live = admin.getTableInfo(livePath)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        try {
            admin.getTableInfo(shadowPath).get(timeoutMs, TimeUnit.MILLISECONDS);
            System.err.println("eod-controller: shadow " + shadow + " already exists — "
                    + "drop it (or rename) before re-running the drill");
            return false;
        } catch (Exception e) {
            // shadow absent — proceed
        }

        Schema schema = live.getSchema();
        TableDescriptor.Builder tb = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(live.getNumBuckets(),
                        live.getBucketKeys().toArray(String[]::new));
        // Keep the create-time options that matter for a rewrite: the extended
        // TTL is the point; kv.format-version rides along; lake stays disabled
        // (create-only — same dev deviation as the live tables).
        String kvFormat = live.getProperties().toMap().get("table.kv.format-version");
        if (kvFormat != null) {
            tb.property("table.kv.format-version", kvFormat);
        }
        tb.property("table.log.ttl", ttlOption(newTtl));
        admin.createTable(shadowPath, tb.build(), false)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        System.out.println("eod-controller: created shadow " + shadow + " ttl=" + newTtl);

        Table liveTable = connection.getTable(livePath);
        Table shadowTable = connection.getTable(shadowPath);
        UpsertWriter writer = shadowTable.newUpsert().createWriter();
        long copied = 0;
        try {
            for (int b = 0; b < live.getNumBuckets(); b++) {
                TableBucket bucket = new TableBucket(live.getTableId(), b);
                try (BatchScanner scanner = liveTable.newScan()
                             .limit(Integer.MAX_VALUE)
                             .createBatchScanner(bucket);
                     CloseableIterator<InternalRow> it =
                             scanner.pollBatch(Duration.ofMillis(250))) {
                    while (it.hasNext()) {
                        InternalRow row = it.next();
                        writer.upsert(GenericRow.of(toValues(row, schema)))
                                .get(timeoutMs, TimeUnit.MILLISECONDS);
                        copied++;
                    }
                }
            }
        } finally {
            writer.flush();
        }
        long liveCount = scanCount(liveTable, live, timeoutMs);
        long shadowCount = scanCount(shadowTable,
                admin.getTableInfo(shadowPath).get(timeoutMs, TimeUnit.MILLISECONDS), timeoutMs);
        System.out.println("eod-controller: shadow copy rows=" + copied
                + " liveCount=" + liveCount + " shadowCount=" + shadowCount);
        if (liveCount != shadowCount || copied != liveCount) {
            System.err.println("eod-controller: shadow copy does not reconcile "
                    + "(live=" + liveCount + " shadow=" + shadowCount + " copied=" + copied + ")");
            return false;
        }
        return true;
    }

    /** Row → Object[] in schema order (raw-client upsert values). */
    private static Object[] toValues(InternalRow row, Schema schema) {
        List<Schema.Column> columns = schema.getColumns();
        Object[] out = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            if (row.isNullAt(i)) {
                out[i] = null;
                continue;
            }
            DataType type = columns.get(i).getDataType();
            out[i] = switch (type.getTypeRoot()) {
                case STRING -> row.getString(i);
                case BIGINT -> row.getLong(i);
                case INTEGER -> row.getInt(i);
                case BYTES -> row.getBytes(i);
                case DOUBLE -> row.getDouble(i);
                case BOOLEAN -> row.getBoolean(i);
                case FLOAT -> row.getFloat(i);
                default -> throw new IllegalStateException(
                        "rewrite copy: unsupported type " + type + " at column " + i);
            };
        }
        return out;
    }

    private static long scanCount(Table table, TableInfo info, long timeoutMs) throws Exception {
        long count = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket bucket = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(bucket);
                 CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
            }
        }
        return count;
    }

    /** Render a Duration as a Fluss TTL option value (2d / 1h / 30m / 5000ms). */
    static String ttlOption(Duration d) {
        long dayMs = Duration.ofDays(1).toMillis();
        long hourMs = Duration.ofHours(1).toMillis();
        long minuteMs = Duration.ofMinutes(1).toMillis();
        if (d.toMillis() % dayMs == 0) {
            return d.toDays() + "d";
        }
        if (d.toMillis() % hourMs == 0) {
            return d.toHours() + "h";
        }
        if (d.toMillis() % minuteMs == 0) {
            return d.toMinutes() + "m";
        }
        return d.toMillis() + "ms";
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static Duration liveTtl(Admin admin, String database, String table,
            Duration fallback) {
        try {
            TableInfo info = admin.getTableInfo(TablePath.of(database, table))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            String ttl = info.getProperties().toMap().get("table.log.ttl");
            if (ttl != null && !ttl.isBlank()) {
                return EodRetentionPolicy.parseTtl(ttl);
            }
        } catch (Exception e) {
            // metadata unavailable — fall back below
        }
        System.err.println("eod-controller: no live table.log.ttl for " + table
                + " — using fallback " + fallback);
        return fallback;
    }

    private static String token() {
        String host = System.getenv().getOrDefault("HOSTNAME", "unknown");
        return host + "-" + System.nanoTime();
    }

    private static int usage() {
        System.err.println("""
                usage: eod-controller <status|run|extend|reconcile|reset> [options]
                  --bootstrap <addr>   (env FLUSS_BOOTSTRAP, default localhost:9123)
                  --database <db>      (env FLUSS_DATABASE, default default)
                  --state-table <name> (env EOD_STATE_TABLE, default eod_offload_state)
                  --tables <t1,t2>     (env EOD_TABLES — EOD-eligible tables)
                  --ttl <ttl>          (env EOD_TTL, default 2d — live-TTL fallback)
                  --safety-floor <ttl> (env EOD_SAFETY_FLOOR, default 7d)
                  --extension <ttl>    (env EOD_EXTENSION, default 30d)
                  --lease-ttl <ttl>    (env EOD_LEASE_TTL, default 30m)
                  --zone <zone>        (env EOD_ZONE, default Asia/Kolkata)
                  --run-date <date>    (run: trading date, default today in --zone)
                  --schema-version <v> (env EOD_SCHEMA_VERSION, default 1)
                  --offload none|mock  (env EOD_OFFLOAD, default none — fail-closed)
                  --table <name>       (reset: single table scope)
                  --apply              (extend: perform the shadow rewrite drill)
                  --dry-run            (run/extend: print, don't write)
                  --approve            (reset: destructive approval)
                exit: 0 ok, 1 failure, 2 extension/retryable, 3 pending work,
                      4 usage/approval, 5 lease held""");
        return 4;
    }

    // ── options ───────────────────────────────────────────────────────────

    record Options(String subcommand, String bootstrap, String database, String stateTable,
                   List<String> tables, Duration ttlDefault, Duration safetyFloor,
                   Duration extension, Duration leaseTtl, String zone, String runDate,
                   String schemaVersion, String offloadMode, String singleTable,
                   boolean apply, boolean dryRun, boolean approve) {

        static Options parse(String[] args) {
            if (args.length == 0) {
                throw new IllegalArgumentException("subcommand required "
                        + "(status|run|extend|reconcile|reset)");
            }
            String subcommand = args[0];
            String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
            String database = System.getenv().getOrDefault("FLUSS_DATABASE", "default");
            String stateTable = System.getenv().getOrDefault("EOD_STATE_TABLE",
                    "eod_offload_state");
            String tablesRaw = System.getenv().getOrDefault("EOD_TABLES", null);
            Duration ttlDefault = parseEnvTtl("EOD_TTL", Duration.ofDays(2));
            Duration safetyFloor = parseEnvTtl("EOD_SAFETY_FLOOR", Duration.ofDays(7));
            Duration extension = parseEnvTtl("EOD_EXTENSION", Duration.ofDays(30));
            Duration leaseTtl = parseEnvTtl("EOD_LEASE_TTL", Duration.ofMinutes(30));
            String zone = System.getenv().getOrDefault("EOD_ZONE", "Asia/Kolkata");
            String runDate = null;
            String schemaVersion = System.getenv().getOrDefault("EOD_SCHEMA_VERSION", "1");
            String offloadMode = System.getenv().getOrDefault("EOD_OFFLOAD", "none");
            String singleTable = null;
            boolean apply = false;
            boolean dryRun = false;
            boolean approve = false;

            List<String> tableArgs = new ArrayList<>();
            for (int i = 1; i < args.length; i++) {
                switch (args[i]) {
                    case "--bootstrap" -> bootstrap = args[++i];
                    case "--database" -> database = args[++i];
                    case "--state-table" -> stateTable = args[++i];
                    case "--tables" -> {
                        for (String t : args[++i].split(",")) {
                            String trimmed = t.trim();
                            if (!trimmed.isEmpty()) {
                                tableArgs.add(trimmed);
                            }
                        }
                    }
                    case "--ttl" -> ttlDefault = EodRetentionPolicy.parseTtl(args[++i]);
                    case "--safety-floor" -> safetyFloor = EodRetentionPolicy.parseTtl(args[++i]);
                    case "--extension" -> extension = EodRetentionPolicy.parseTtl(args[++i]);
                    case "--lease-ttl" -> leaseTtl = EodRetentionPolicy.parseTtl(args[++i]);
                    case "--zone" -> zone = args[++i];
                    case "--run-date" -> runDate = args[++i];
                    case "--schema-version" -> schemaVersion = args[++i];
                    case "--offload" -> offloadMode = args[++i];
                    case "--table" -> singleTable = args[++i];
                    case "--apply" -> apply = true;
                    case "--dry-run" -> dryRun = true;
                    case "--approve" -> approve = true;
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            if (!offloadMode.equalsIgnoreCase("none") && !offloadMode.equalsIgnoreCase("mock")) {
                throw new IllegalArgumentException("--offload must be none or mock, got "
                        + offloadMode);
            }
            if (runDate != null && !runDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
                throw new IllegalArgumentException("--run-date must be yyyy-MM-dd, got " + runDate);
            }
            List<String> tables = tableArgs.isEmpty() && tablesRaw != null
                    ? List.of(tablesRaw.split(","))
                    : tableArgs.isEmpty() ? DEFAULT_TABLES : List.copyOf(tableArgs);
            return new Options(subcommand, bootstrap, database, stateTable, tables,
                    ttlDefault, safetyFloor, extension, leaseTtl, zone, runDate, schemaVersion,
                    offloadMode, singleTable, apply, dryRun, approve);
        }

        private static Duration parseEnvTtl(String key, Duration fallback) {
            String raw = System.getenv(key);
            if (raw == null || raw.isBlank()) {
                return fallback;
            }
            return EodRetentionPolicy.parseTtl(raw);
        }
    }
}
