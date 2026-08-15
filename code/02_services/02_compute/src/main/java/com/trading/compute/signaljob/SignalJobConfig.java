package com.trading.compute.signaljob;

import com.trading.common.config.PlatformConfig;
import com.trading.common.schema.CandleTableSchema;
import java.io.Serializable;
import java.util.Map;

/**
 * Validated runtime configuration for the Signal job (compute path).
 *
 * <p><b>Load-bearing values are pinned, not tuned.</b> The fixed-scope contract
 * (docs/08_implementation/01-foundation.md, PlatformConfig javadoc) requires the
 * two correctness-critical constants — {@code DEDUP_TTL_MS} and
 * {@code CANDLE_WINDOW_MS} — plus the checkpoint contract
 * (REQ-FC-006) to be exactly pinned or the job MUST fail at startup. A missing
 * required value also aborts: "no unsafe default may be substituted"
 * (PlatformConfig.requirePresent). All other keys are tuning parameters with
 * documented defaults from the Signal-job dossier.
 *
 * <p>The fixed-delay restart strategy
 * ({@code RESTART_MAX_ATTEMPTS=3}, {@code RESTART_DELAY_MS=30000}) is
 * production-pinned: in {@code DEPLOYMENT_ENV=production} the two keys must be
 * present and exactly equal the PlatformConfig pins or startup fails — a
 * deployment cannot silently raise the retry budget. Dev keeps them as
 * overridable tuning defaults (failure-injection integration tests rely on
 * low attempts).
 *
 * <p>Build via {@link #fromEnv()} in production; {@link #from(Map)} is exposed
 * so the rejection rules are unit-testable without touching {@code System.getenv}.
 *
 * <p><b>Signal dual-sink (DEC-035, tracker 14 re-scoped P2).</b>
 * {@code SIGNAL_CANDIDATES_TABLE} (default {@code Signal_Candidates}) is the
 * append-only LOG; {@code SIGNAL_CURRENT_TABLE} (default
 * {@code Signal_Candidates_current}) is the KV current-state projection. The
 * canonical identity — {@code SIGNAL_STRATEGY_ID},
 * {@code SIGNAL_STRATEGY_VERSION}, {@code SIGNAL_RULE_ID} — defaults to the
 * pinned {@link SignalCandidatesTableColumns} canonical constants, so the
 * default config emits rows the KV filter accepts; an override changes the
 * identity and those rows are filtered from the KV sink (LOG keeps them) and
 * counted via {@code compute.signal.kv.filtered.noncanonical}.
 *
 * <p><b>Fail-closed startup mode (CANDLE-KV-REPLAY-001 A3.3).</b> A restart
 * must either restore from a checkpoint ({@code STATE_RECOVERY_PATH}, mode
 * {@link StartupMode#RESTORE}) or explicitly accept an offset-0 full replay
 * ({@code ALLOW_FULL_REPLAY=true}, mode {@link StartupMode#FULL_REPLAY}).
 * Neither, both, a blank restore path, or an invalid boolean all FAIL startup
 * — a silent offset-0 replay is forbidden (it re-emits the whole backlog,
 * balloons dedup state past the pinned checkpoint contract, and appends
 * duplicate candle rows to the immutable LOG; observed 2026-08-10).
 *
 * <p>Serializable: the config is a field of every operator function and must
 * survive Flink's closure cleaning and serialization to task managers.
 */
public record SignalJobConfig(
        String bootstrapServers,
        String database,
        String rawTable,
        String candleTable,
        String rawSchemaVersion,
        String algorithmVersion,
        String configurationVersion,
        String candleSchemaVersion,
        long dedupTtlMs,
        long candleWindowMs,
        long outOfOrderMs,
        long allowedLatenessMs,
        long sourceIdleMs,
        long sourceIdleAlertMs,
        long checkpointIntervalMs,
        long checkpointTimeoutMs,
        int maxConcurrentCheckpoints,
        int restartMaxAttempts,
        long restartDelayMs,
        String checkpointDir,
        String signalCandidatesTable,
        String signalCurrentTable,
        String signalStrategyId,
        String signalStrategyVersion,
        String signalRuleId,
        int signalLookbackCandles,
        long signalQuantity,
        String otelCollectorHost,
        String stateRecoveryPath,
        boolean allowFullReplay,
        String deploymentEnv,
        String stateBackend,
        String savepointDir,
        String stateBackendLocalDirs,
        boolean stateBackendManagedMemory,
        int parallelism,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey,
        String s3Region,
        boolean s3PathStyle,
        long sinkWriteStallTimeoutMs,
        String tradeDecisionsTable,
        String tradeInstructionStateTable,
        boolean tradeDecisionsEnabled,
        String dedupStateTable,
        long dedupCacheMaxEntries,
        long dedupCacheMaxBytes,
        long dedupWriteBatchMs,
        long dedupWriteBatchSize,
        long dedupCleanupIntervalMs) implements Serializable {

    public static SignalJobConfig fromEnv() {
        return from(System.getenv());
    }

    /** Builds a validated config from an environment map (production: {@code System.getenv()}). */
    public static SignalJobConfig from(Map<String, String> env) {
        StartupMode mode = validateStartupMode(env);
        return new SignalJobConfig(
                env.getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123"),
                env.getOrDefault("FLUSS_DATABASE", "default"),
                env.getOrDefault("RAW_TABLE", "raw_table_1"),
                env.getOrDefault("CANDLE_TABLE", "feature_candles_15s"),
                env.getOrDefault("RAW_SCHEMA_VERSION", PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION),
                requireCanonicalVersion(env, "ALGORITHM_VERSION",
                        CandleTableSchema.CANONICAL_ALGORITHM_VERSION),
                requireCanonicalVersion(env, "CONFIGURATION_VERSION",
                        CandleTableSchema.CANONICAL_CONFIGURATION_VERSION),
                env.getOrDefault("CANDLE_SCHEMA_VERSION", "2"),
                requirePinnedLong(env, "DEDUP_TTL_MS", PlatformConfig.DEDUP_TTL_MS),
                requirePinnedLong(env, "CANDLE_WINDOW_MS", PlatformConfig.CANDLE_WINDOW_MS),
                longValue(env, "WATERMARK_OUT_OF_ORDER_MS", 5_000L),
                longValue(env, "ALLOWED_LATENESS_MS", 5_000L),
                longValue(env, "SOURCE_IDLE_MS", 15_000L),
                sourceIdleAlertMs(env),
                requirePinnedLong(env, "CHECKPOINT_INTERVAL_MS", PlatformConfig.CHECKPOINT_INTERVAL_MS),
                requirePinnedLong(env, "CHECKPOINT_TIMEOUT_MS", PlatformConfig.CHECKPOINT_TIMEOUT_MS),
                requirePinnedInt(env, "MAX_CONCURRENT_CHECKPOINTS", PlatformConfig.MAX_CONCURRENT_CHECKPOINTS),
                restartMaxAttempts(env),
                restartDelayMs(env),
                checkpointDir(env),
                env.getOrDefault("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates"),
                env.getOrDefault("SIGNAL_CURRENT_TABLE", "Signal_Candidates_current"),
                env.getOrDefault("SIGNAL_STRATEGY_ID",
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID),
                env.getOrDefault("SIGNAL_STRATEGY_VERSION",
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION),
                env.getOrDefault("SIGNAL_RULE_ID",
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                signalLookbackCandles(env),
                signalQuantity(env),
                env.getOrDefault("OTEL_COLLECTOR_HOST", "otel-collector:4318"),
                stateRecoveryPath(env),
                mode == StartupMode.FULL_REPLAY,
                deploymentEnv(env),
                stateBackend(env),
                savepointDir(env),
                stateBackendLocalDirs(env),
                stateBackendManagedMemory(env),
                parallelism(env),
                s3Endpoint(env),
                s3AccessKey(env),
                s3SecretKey(env),
                s3Region(env),
                s3PathStyle(env),
                sinkWriteStallTimeoutMs(env),
                env.getOrDefault("TRADE_DECISIONS_TABLE", "Trade_Decisions"),
                env.getOrDefault("TRADE_INSTRUCTION_STATE_TABLE", "trade_instruction_state"),
                booleanValue(env, "TRADE_DECISIONS_ENABLED", false),
                env.getOrDefault("DEDUP_STATE_TABLE", "fingerprint_dedup"),
                positiveLong(env, "DEDUP_CACHE_MAX_ENTRIES", 250_000L),
                positiveLong(env, "DEDUP_CACHE_MAX_BYTES", 33_554_432L),
                positiveLong(env, "DEDUP_WRITE_BATCH_MS", 250L),
                positiveLong(env, "DEDUP_WRITE_BATCH_SIZE", 5_000L),
                positiveLong(env, "DEDUP_CLEANUP_INTERVAL_MS", 60_000L));
    }

    /**
     * The validated startup mode. Exactly one of the two must be active after
     * {@link #from(Map)} passes (CANDLE-KV-REPLAY-001 A3.3): {@link
     * StartupMode#RESTORE} resumes from the last checkpoint of the previous
     * run; {@link StartupMode#FULL_REPLAY} explicitly accepts the offset-0
     * replay cost (state blowup, LOG duplicates, checkpoint risk) — never the
     * default.
     */
    public StartupMode startupMode() {
        return stateRecoveryPath == null ? StartupMode.FULL_REPLAY : StartupMode.RESTORE;
    }

    /** Startup-mode gate (CANDLE-KV-REPLAY-001 A3.3). */
    public enum StartupMode {
        /** Resume from STATE_RECOVERY_PATH (previous run's last checkpoint). */
        RESTORE,
        /** Explicit offset-0 replay (ALLOW_FULL_REPLAY=true, no restore path). */
        FULL_REPLAY
    }

    /**
     * Fail-closed startup-mode validation: a restart must either restore from
     * a checkpoint or explicitly accept a full replay — a silent offset-0
     * replay is forbidden (it re-emits the whole backlog, balloons the dedup
     * state past the pinned checkpoint contract, and appends duplicate candle
     * rows to the immutable LOG — observed 2026-08-10).
     */
    private static StartupMode validateStartupMode(Map<String, String> env) {
        String path = env.get("STATE_RECOVERY_PATH");
        boolean hasPath;
        if (path == null) {
            hasPath = false;
        } else {
            String trimmed = path.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalStateException(
                        "Config STATE_RECOVERY_PATH is present but blank — a restore path must be a real path "
                                + "(CANDLE-KV-REPLAY-001 A3.3)");
            }
            hasPath = true;
        }

        String replayRaw = env.get("ALLOW_FULL_REPLAY");
        boolean replay;
        if (replayRaw == null) {
            replay = false;
        } else {
            String trimmed = replayRaw.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalStateException(
                        "Config ALLOW_FULL_REPLAY is present but blank — use 'true' or 'false' "
                                + "(CANDLE-KV-REPLAY-001 A3.3)");
            }
            if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
                throw new IllegalStateException(
                        "Config ALLOW_FULL_REPLAY must be 'true' or 'false' (case-insensitive), got '"
                                + trimmed + "' — no unsafe default may be substituted (CANDLE-KV-REPLAY-001 A3.3)");
            }
            replay = Boolean.parseBoolean(trimmed);
        }

        if (hasPath && replay) {
            throw new IllegalStateException(
                    "Config STATE_RECOVERY_PATH and ALLOW_FULL_REPLAY=true are both set — a restore and a "
                            + "full replay cannot be combined (CANDLE-KV-REPLAY-001 A3.3); unset one of them");
        }
        if (!hasPath && !replay) {
            throw new IllegalStateException(
                    "Missing startup mode: set STATE_RECOVERY_PATH (restore) or ALLOW_FULL_REPLAY=true "
                            + "(explicit full replay) — a silent offset-0 replay is forbidden "
                            + "(CANDLE-KV-REPLAY-001 A3.3)");
        }
        return hasPath ? StartupMode.RESTORE : StartupMode.FULL_REPLAY;
    }

    /** Trims the restore path (validation already rejected blank values). */
    private static String stateRecoveryPath(Map<String, String> env) {
        String raw = env.get("STATE_RECOVERY_PATH");
        return raw == null ? null : raw.trim();
    }

    private static int signalLookbackCandles(Map<String, String> env) {
        int value = intValue(env, "SIGNAL_LOOKBACK_CANDLES", 20);
        if (value < 2) {
            throw new IllegalStateException("Config SIGNAL_LOOKBACK_CANDLES must be >= 2 "
                    + "(the rule compares against the previous completed candles), got " + value);
        }
        return value;
    }

    private static long signalQuantity(Map<String, String> env) {
        long value = longValue(env, "SIGNAL_QUANTITY", 1L);
        if (value <= 0) {
            throw new IllegalStateException("Config SIGNAL_QUANTITY must be > 0, got " + value);
        }
        return value;
    }

    /**
     * Fixed-delay restart strategy (CHECKPOINT_RESTART_STRATEGY, dossier
     * config contract). Dev defaults {@code RESTART_MAX_ATTEMPTS} to
     * {@link PlatformConfig#RESTART_MAX_ATTEMPTS} and allows overrides
     * (failure-injection integration tests use low attempts to fail fast);
     * {@code DEPLOYMENT_ENV=production} pins the exact value and requires it
     * explicit — a deployment cannot silently raise the retry budget or widen
     * the delay (bounded retry only, never unbounded).
     */
    private static int restartMaxAttempts(Map<String, String> env) {
        if ("production".equals(deploymentEnv(env))) {
            return requirePinnedInt(env, "RESTART_MAX_ATTEMPTS",
                    PlatformConfig.RESTART_MAX_ATTEMPTS);
        }
        return intValue(env, "RESTART_MAX_ATTEMPTS", PlatformConfig.RESTART_MAX_ATTEMPTS);
    }

    /**
     * Fixed-delay restart delay (see {@link #restartMaxAttempts(Map)}): dev
     * tuning default 30 s, production-pinned to
     * {@link PlatformConfig#RESTART_DELAY_MS} and required explicit.
     */
    private static long restartDelayMs(Map<String, String> env) {
        if ("production".equals(deploymentEnv(env))) {
            return requirePinnedLong(env, "RESTART_DELAY_MS", PlatformConfig.RESTART_DELAY_MS);
        }
        return longValue(env, "RESTART_DELAY_MS", PlatformConfig.RESTART_DELAY_MS);
    }

    private static long requirePinnedLong(Map<String, String> env, String key, long pinned) {
        String raw = env.get(key);
        if (raw == null) {
            throw new IllegalStateException("Missing required config " + key
                    + " (pinned to " + pinned + ") — no unsafe default may be substituted");
        }
        long value = Long.parseLong(raw.trim());
        if (value != pinned) {
            throw new IllegalStateException("Config " + key + " must equal " + pinned
                    + " (fixed scope), got " + value);
        }
        return value;
    }

    private static int requirePinnedInt(Map<String, String> env, String key, int pinned) {
        return (int) requirePinnedLong(env, key, pinned);
    }

    /**
     * Canonical version-column gate (tracker 14 P2 — CANDLE-CANONICAL-001).
     * The emitted candle KV rows carry the algorithm/configuration pair; a
     * <em>deviating</em> pair must fail startup rather than silently change
     * row identity for replay evidence. A missing key falls back to the
     * canonical default (documented in the record javadoc — the default IS
     * the canonical pair); a blank or non-canonical value is fatal. Changing
     * the pair is a governed change in {@link CandleTableSchema}, not a
     * tuning knob.
     */
    private static String requireCanonicalVersion(Map<String, String> env, String key,
            String canonical) {
        String raw = env.get(key);
        if (raw == null) {
            return canonical; // documented default — the default IS the canonical pair
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config " + key + " is present but blank — the "
                    + "canonical pair must be explicit (tracker 14 P2, CANDLE-CANONICAL-001)");
        }
        if (!canonical.equals(trimmed)) {
            throw new IllegalStateException("Config " + key + " must equal the canonical value '"
                    + canonical + "' (tracker 14 P2, CANDLE-CANONICAL-001), got '" + trimmed
                    + "' — a deviating version column would change emitted row identity "
                    + "and corrupt replay evidence");
        }
        return trimmed;
    }

    /**
     * Deployment environment (tracker 14 P4.1). Defaults to {@code dev} — the
     * fail-closed direction is toward dev-local, never silently toward
     * production: a production launch must set {@code DEPLOYMENT_ENV=production}
     * explicitly, which then enforces the RocksDB backend and durable S3
     * checkpoint/savepoint URIs (P4.2). The default keeps the live dev run
     * (HashMap state, local checkpoints) restart-compatible.
     */
    private static String deploymentEnv(Map<String, String> env) {
        String raw = env.get("DEPLOYMENT_ENV");
        if (raw == null) {
            return "dev";
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config DEPLOYMENT_ENV is present but blank — "
                    + "use 'dev' or 'production'");
        }
        if (!"dev".equals(trimmed) && !"production".equals(trimmed)) {
            throw new IllegalStateException("Config DEPLOYMENT_ENV must be 'dev' or 'production', "
                    + "got '" + trimmed + "' — no unsafe default may be substituted");
        }
        return trimmed;
    }

    /**
     * State backend (tracker 14 P4.1). {@code rocksdb} is the pinned production
     * backend; {@code hashmap} is dev-only and FAILS in production (heap state
     * blew past the pinned checkpoint contract on 2026-08-10). The dev default
     * is {@code hashmap} to keep the live dev run's checkpoints
     * (HashMapStateBackend) restorable on the next operator restart; dev may
     * choose {@code rocksdb} explicitly.
     */
    private static String stateBackend(Map<String, String> env) {
        String envRaw = env.get("STATE_BACKEND");
        String backend;
        if (envRaw == null) {
            backend = "production".equals(env.get("DEPLOYMENT_ENV") == null
                    ? "dev" : env.get("DEPLOYMENT_ENV").trim()) ? "rocksdb" : "hashmap";
        } else {
            backend = envRaw.trim();
        }
        if (backend.isEmpty()) {
            throw new IllegalStateException("Config STATE_BACKEND is present but blank — "
                    + "use 'rocksdb' or 'hashmap'");
        }
        if (!"rocksdb".equals(backend) && !"hashmap".equals(backend)) {
            throw new IllegalStateException("Config STATE_BACKEND must be 'rocksdb' (production "
                    + "pin) or 'hashmap' (dev only), got '" + backend + "' — no unsafe default "
                    + "may be substituted");
        }
        String envName = deploymentEnv(env);
        if ("production".equals(envName) && "hashmap".equals(backend)) {
            throw new IllegalStateException("Config STATE_BACKEND=hashmap is forbidden in "
                    + "DEPLOYMENT_ENV=production — heap/HashMap state blew past the pinned "
                    + "checkpoint contract on 2026-08-10 (tracker 14 P4.1); use rocksdb");
        }
        return backend;
    }

    /**
     * Durable checkpoint URI (tracker 14 P4.2). Production REQUIRES an S3
     * object-store URI ({@code s3://} or {@code s3a://}) — local {@code /tmp}
     * checkpoints silently evaporate on container loss and must never be
     * substituted in production. Dev accepts any path (the live run uses
     * {@code /tmp/signaljob-checkpoints}).
     */
    private static String checkpointDir(Map<String, String> env) {
        String raw = env.get("CHECKPOINT_DIR");
        String dir = raw == null ? null : raw.trim();
        if ("production".equals(deploymentEnv(env))) {
            if (dir == null || dir.isEmpty()) {
                throw new IllegalStateException("Missing required config CHECKPOINT_DIR in "
                        + "DEPLOYMENT_ENV=production — durable S3 checkpoint storage is mandatory "
                        + "(tracker 14 P4.2)");
            }
            if (!dir.startsWith("s3://") && !dir.startsWith("s3a://")) {
                throw new IllegalStateException("Config CHECKPOINT_DIR must be an S3 object-store "
                        + "URI (s3:// or s3a://) in DEPLOYMENT_ENV=production, got '" + dir
                        + "' — a local path silently substitutes /tmp (tracker 14 P4.2)");
            }
        }
        return dir;
    }

    /**
     * Durable savepoint URI (tracker 14 P4.2), kept separate from the
     * checkpoint directory. Production requires S3 when set; dev accepts any.
     */
    private static String savepointDir(Map<String, String> env) {
        String raw = env.get("SAVEPOINT_DIR");
        String dir = raw == null ? null : raw.trim();
        if ("production".equals(deploymentEnv(env)) && dir != null && !dir.isEmpty()
                && !dir.startsWith("s3://") && !dir.startsWith("s3a://")) {
            throw new IllegalStateException("Config SAVEPOINT_DIR must be an S3 object-store URI "
                    + "in DEPLOYMENT_ENV=production, got '" + dir + "' (tracker 14 P4.2)");
        }
        return dir;
    }

    /** RocksDB local state directory (fast disk), optional (tracker 14 P4.1). */
    private static String stateBackendLocalDirs(Map<String, String> env) {
        String raw = env.get("STATE_BACKEND_LOCAL_DIRS");
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config STATE_BACKEND_LOCAL_DIRS is present but blank "
                    + "(tracker 14 P4.1)");
        }
        return trimmed;
    }

    /**
     * RocksDB managed-memory toggle (tracker 14 P4.1); explicit boolean, no
     * unsafe default substitution when the key is present but unparsable.
     */
    private static boolean stateBackendManagedMemory(Map<String, String> env) {
        String raw = env.get("STATE_BACKEND_MANAGED_MEMORY");
        if (raw == null) {
            return true;
        }
        String trimmed = raw.trim();
        if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
            throw new IllegalStateException("Config STATE_BACKEND_MANAGED_MEMORY must be 'true' "
                    + "or 'false' (case-insensitive), got '" + trimmed + "'");
        }
        return Boolean.parseBoolean(trimmed);
    }

    /** Explicit task parallelism (tracker 14 P4.1). */
    private static int parallelism(Map<String, String> env) {
        int value = intValue(env, "PARALLELISM", 1);
        if (value <= 0) {
            throw new IllegalStateException("Config PARALLELISM must be > 0, got " + value);
        }
        return value;
    }

    /**
     * Object-store checkpoint/savepoint endpoint (tracker 14 P4.2). Non-null
     * ONLY when CHECKPOINT_DIR or SAVEPOINT_DIR is an S3 object-store URI
     * ({@code s3://}/{@code s3a://}). Fail-closed: an object-store URI without
     * an endpoint + credentials is rejected at startup — credentials come from
     * secret injection via env (never committed files, never logged). The
     * endpoint comes from {@code S3_ENDPOINT}, with {@code R2_ENDPOINT} as the
     * Cloudflare-R2 fallback (R2 speaks the S3 API; the endpoint is the
     * jurisdiction URL, e.g. {@code https://<account>.r2.cloudflarestorage.com}).
     */
    private static String s3Endpoint(Map<String, String> env) {
        String cp = env.get("CHECKPOINT_DIR");
        String sp = env.get("SAVEPOINT_DIR");
        boolean objectStore = (cp != null && (cp.startsWith("s3://") || cp.startsWith("s3a://")))
                || (sp != null && (sp.startsWith("s3://") || sp.startsWith("s3a://")));
        if (!objectStore) {
            return null;
        }
        String endpoint = env.get("S3_ENDPOINT");
        if (endpoint == null || endpoint.trim().isEmpty()) {
            endpoint = env.get("R2_ENDPOINT");
        }
        String access = env.get("AWS_ACCESS_KEY_ID");
        String secret = env.get("AWS_SECRET_ACCESS_KEY");
        if (endpoint == null || endpoint.trim().isEmpty()
                || access == null || access.trim().isEmpty()
                || secret == null || secret.trim().isEmpty()) {
            throw new IllegalStateException("Config CHECKPOINT_DIR/SAVEPOINT_DIR uses an S3 "
                    + "object-store URI but S3_ENDPOINT (or R2_ENDPOINT), AWS_ACCESS_KEY_ID and "
                    + "AWS_SECRET_ACCESS_KEY are not all set — checkpoint credentials must come "
                    + "from secret injection, never committed files (tracker 14 P4.2)");
        }
        return endpoint.trim();
    }

    /** Static access key, present only when {@link #s3Endpoint(Map)} returned non-null. */
    private static String s3AccessKey(Map<String, String> env) {
        String raw = env.get("AWS_ACCESS_KEY_ID");
        return raw == null ? null : raw.trim();
    }

    /** Static secret key, present only when {@link #s3Endpoint(Map)} returned non-null. */
    private static String s3SecretKey(Map<String, String> env) {
        String raw = env.get("AWS_SECRET_ACCESS_KEY");
        return raw == null ? null : raw.trim();
    }

    /**
     * S3 signing region (tracker 14 P4.2). Defaults to {@code auto} — the
     * correct choice for Cloudflare R2 (signing is per-endpoint, not
     * region-scoped). Explicit AWS regions (e.g. {@code us-east-1}) override.
     */
    private static String s3Region(Map<String, String> env) {
        String raw = env.get("AWS_REGION");
        return raw == null || raw.trim().isEmpty() ? "auto" : raw.trim();
    }

    /**
     * Path-style S3 addressing (tracker 14 P4.2). Defaults to {@code true} —
     * required for R2 (no virtual-hosted-style buckets); explicit
     * {@code false} opts into virtual-hosted style for real AWS.
     */
    private static boolean s3PathStyle(Map<String, String> env) {
        String raw = env.get("S3_PATH_STYLE");
        if (raw == null) {
            return true;
        }
        String trimmed = raw.trim();
        if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
            throw new IllegalStateException("Config S3_PATH_STYLE must be 'true' or 'false' "
                    + "(case-insensitive), got '" + trimmed + "'");
        }
        return Boolean.parseBoolean(trimmed);
    }

    /**
     * Sink write-path stall bound (tracker 14 box 682/116, governed pin in
     * {@link PlatformConfig#SINK_WRITE_STALL_TIMEOUT_MS}). Defaults to the
     * pinned 15000 ms; a present value must parse and be strictly positive —
     * a non-positive bound would let a stalled sink hang the job forever
     * (the exact failure the guard exists to bound).
     */
    private static long sinkWriteStallTimeoutMs(Map<String, String> env) {
        long value = longValue(env, "SINK_WRITE_STALL_TIMEOUT_MS",
                PlatformConfig.SINK_WRITE_STALL_TIMEOUT_MS);
        if (value <= 0) {
            throw new IllegalStateException("Config SINK_WRITE_STALL_TIMEOUT_MS must be > 0 "
                    + "(pinned default " + PlatformConfig.SINK_WRITE_STALL_TIMEOUT_MS
                    + "), got " + value);
        }
        return value;
    }

    /**
     * Source idle-at-tail alert threshold (tracker 14 P7/P10, 2026-08-13).
     * Defaults to the governed default in {@link PlatformConfig#SOURCE_IDLE_ALERT_MS}
     * (60000 ms); a present value must parse and be strictly positive — a
     * non-positive threshold would alert on every scheduler pause, and a
     * threshold below SOURCE_IDLE_MS would alert during normal quiet-split
     * watermark idleness.
     */
    private static long sourceIdleAlertMs(Map<String, String> env) {
        long value = longValue(env, "SOURCE_IDLE_ALERT_MS", PlatformConfig.SOURCE_IDLE_ALERT_MS);
        if (value <= 0) {
            throw new IllegalStateException("Config SOURCE_IDLE_ALERT_MS must be > 0 "
                    + "(pinned default " + PlatformConfig.SOURCE_IDLE_ALERT_MS
                    + "), got " + value);
        }
        return value;
    }

    /**
     * Strict boolean (no unsafe default substitution when the key is present
     * but unparsable) — the pattern of {@link #stateBackendManagedMemory}.
     * Used for the SCH-19 decision-sink gate {@code TRADE_DECISIONS_ENABLED}
     * (default {@code false}: the ranking feed does not exist yet; the
     * machinery stays off until wired).
     */
    private static boolean booleanValue(Map<String, String> env, String key,
            boolean defaultValue) {
        String raw = env.get(key);
        if (raw == null) {
            return defaultValue;
        }
        String trimmed = raw.trim();
        if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
            throw new IllegalStateException("Config " + key + " must be 'true' or 'false' "
                    + "(case-insensitive), got '" + trimmed + "'");
        }
        return Boolean.parseBoolean(trimmed);
    }

    /**
     * DEC-038 dedup tuning keys (DEDUP_CACHE_* / DEDUP_WRITE_* /
     * DEDUP_CLEANUP_*): defaulted and validated at startup — a missing key
     * falls back to the documented starting value, a present non-positive
     * value is fatal (a zero cache bound or write cadence would break the
     * bounded-cache contract; see 04-signal-job.md §Design — fingerprint_dedup).
     */
    private static long positiveLong(Map<String, String> env, String key, long defaultValue) {
        long value = longValue(env, key, defaultValue);
        if (value <= 0) {
            throw new IllegalStateException("Config " + key + " must be > 0 "
                    + "(default " + defaultValue + "), got " + value);
        }
        return value;
    }

    private static long longValue(Map<String, String> env, String key, long defaultValue) {
        String raw = env.get(key);
        return raw == null ? defaultValue : Long.parseLong(raw.trim());
    }

    private static int intValue(Map<String, String> env, String key, int defaultValue) {
        String raw = env.get(key);
        return raw == null ? defaultValue : Integer.parseInt(raw.trim());
    }
}
