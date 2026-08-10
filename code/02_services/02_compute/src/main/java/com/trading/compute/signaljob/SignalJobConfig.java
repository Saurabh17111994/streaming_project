package com.trading.compute.signaljob;

import com.trading.common.config.PlatformConfig;
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
 * <p>Build via {@link #fromEnv()} in production; {@link #from(Map)} is exposed
 * so the rejection rules are unit-testable without touching {@code System.getenv}.
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
        String candleCurrentTable,
        String rawSchemaVersion,
        String algorithmVersion,
        String configurationVersion,
        String candleSchemaVersion,
        long dedupTtlMs,
        long candleWindowMs,
        long outOfOrderMs,
        long allowedLatenessMs,
        long sourceIdleMs,
        long checkpointIntervalMs,
        long checkpointTimeoutMs,
        int maxConcurrentCheckpoints,
        int restartMaxAttempts,
        long restartDelayMs,
        String checkpointDir,
        String signalCandidatesTable,
        String signalStrategyId,
        String signalStrategyVersion,
        String signalRuleId,
        int signalLookbackCandles,
        long signalQuantity,
        String otelCollectorHost,
        String stateRecoveryPath,
        boolean allowFullReplay) implements Serializable {

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
                env.getOrDefault("CANDLE_CURRENT_TABLE", "feature_candles_15s_current"),
                env.getOrDefault("RAW_SCHEMA_VERSION", PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION),
                env.getOrDefault("ALGORITHM_VERSION", "candle-15s-v1"),
                env.getOrDefault("CONFIGURATION_VERSION", "1.0.0"),
                env.getOrDefault("CANDLE_SCHEMA_VERSION", "2"),
                requirePinnedLong(env, "DEDUP_TTL_MS", PlatformConfig.DEDUP_TTL_MS),
                requirePinnedLong(env, "CANDLE_WINDOW_MS", PlatformConfig.CANDLE_WINDOW_MS),
                longValue(env, "WATERMARK_OUT_OF_ORDER_MS", 5_000L),
                longValue(env, "ALLOWED_LATENESS_MS", 5_000L),
                longValue(env, "SOURCE_IDLE_MS", 15_000L),
                requirePinnedLong(env, "CHECKPOINT_INTERVAL_MS", PlatformConfig.CHECKPOINT_INTERVAL_MS),
                requirePinnedLong(env, "CHECKPOINT_TIMEOUT_MS", PlatformConfig.CHECKPOINT_TIMEOUT_MS),
                requirePinnedInt(env, "MAX_CONCURRENT_CHECKPOINTS", PlatformConfig.MAX_CONCURRENT_CHECKPOINTS),
                intValue(env, "RESTART_MAX_ATTEMPTS", 3),
                longValue(env, "RESTART_DELAY_MS", 30_000L),
                env.get("CHECKPOINT_DIR"),
                env.getOrDefault("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates"),
                env.getOrDefault("SIGNAL_STRATEGY_ID", "simple-breakout"),
                env.getOrDefault("SIGNAL_STRATEGY_VERSION", "1.0.0"),
                env.getOrDefault("SIGNAL_RULE_ID", "breakout-20-bullish-trend"),
                signalLookbackCandles(env),
                signalQuantity(env),
                env.getOrDefault("OTEL_COLLECTOR_HOST", "otel-collector:4318"),
                stateRecoveryPath(env),
                mode == StartupMode.FULL_REPLAY);
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

    private static long longValue(Map<String, String> env, String key, long defaultValue) {
        String raw = env.get(key);
        return raw == null ? defaultValue : Long.parseLong(raw.trim());
    }

    private static int intValue(Map<String, String> env, String key, int defaultValue) {
        String raw = env.get(key);
        return raw == null ? defaultValue : Integer.parseInt(raw.trim());
    }
}
