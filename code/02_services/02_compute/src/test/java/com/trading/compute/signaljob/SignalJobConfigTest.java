package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.config.PlatformConfig;
import com.trading.common.schema.CandleTableSchema;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Fixed-scope and config-contract enforcement (PlatformConfig / REQ-FC-006). */
class SignalJobConfigTest {

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    @Test
    void acceptsPinnedValuesAndDefaultsForTuning() {
        SignalJobConfig cfg = SignalJobConfig.from(env());
        assertEquals(300_000L, cfg.dedupTtlMs());
        assertEquals(15_000L, cfg.candleWindowMs());
        assertEquals(10_000L, cfg.checkpointIntervalMs());
        assertEquals(30_000L, cfg.checkpointTimeoutMs());
        assertEquals(1, cfg.maxConcurrentCheckpoints());
        // documented tuning defaults
        assertEquals(5_000L, cfg.outOfOrderMs());
        assertEquals(5_000L, cfg.allowedLatenessMs());
        assertEquals(15_000L, cfg.sourceIdleMs());
        assertEquals(60_000L, cfg.sourceIdleAlertMs());
        assertEquals(3, cfg.restartMaxAttempts());
        assertEquals(30_000L, cfg.restartDelayMs());
        assertEquals("localhost:9123", cfg.bootstrapServers());
        assertEquals("default", cfg.database());
        assertEquals("raw_table_1", cfg.rawTable());
        assertEquals("feature_candles_15s", cfg.candleTable());
        assertEquals(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION, cfg.rawSchemaVersion());
        assertEquals("2", cfg.candleSchemaVersion());
        // signal detection tuning defaults (DEC-034)
        assertEquals("Signal_Candidates", cfg.signalCandidatesTable());
        assertEquals("simple-breakout", cfg.signalStrategyId());
        assertEquals("1.0.0", cfg.signalStrategyVersion());
        assertEquals("breakout-20-bullish-trend", cfg.signalRuleId());
        assertEquals(20, cfg.signalLookbackCandles());
        assertEquals(1L, cfg.signalQuantity());
        // OTLP collector: compose DNS default, live-run override (process rule 2)
        assertEquals("otel-collector:4318", cfg.otelCollectorHost());
        // state restore: absent by default (first start replays from offset 0)
        assertEquals(null, cfg.stateRecoveryPath());
    }

    @Test
    void honorsStateRecoveryPathOverride() {
        Map<String, String> env = env();
        env.remove("ALLOW_FULL_REPLAY");
        env.put("STATE_RECOVERY_PATH",
                "file:///tmp/signaljob-checkpoints/519394deb5115efbea4ede92b6e9e62a/chk-647");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("file:///tmp/signaljob-checkpoints/519394deb5115efbea4ede92b6e9e62a/chk-647",
                cfg.stateRecoveryPath());
        assertEquals(SignalJobConfig.StartupMode.RESTORE, cfg.startupMode());
    }

    @Test
    void fullReplayFlagYieldsFullReplayModeAndNoRestorePath() {
        SignalJobConfig cfg = SignalJobConfig.from(env());
        assertEquals(SignalJobConfig.StartupMode.FULL_REPLAY, cfg.startupMode());
        assertEquals(true, cfg.allowFullReplay());
        assertEquals(null, cfg.stateRecoveryPath());
    }

    @Test
    void restoreWithExplicitFalseReplayIsRestoreMode() {
        Map<String, String> env = env();
        env.put("STATE_RECOVERY_PATH", "file:///tmp/signaljob-checkpoints/job/chk-1");
        env.put("ALLOW_FULL_REPLAY", "false");
        assertEquals(SignalJobConfig.StartupMode.RESTORE,
                SignalJobConfig.from(env).startupMode());
    }

    @Test
    void rejectsNeitherRestoreNorReplay() {
        Map<String, String> env = env();
        env.remove("ALLOW_FULL_REPLAY");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsBothRestoreAndReplay() {
        Map<String, String> env = env();
        env.put("STATE_RECOVERY_PATH", "file:///tmp/signaljob-checkpoints/job/chk-1");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsBlankRestorePath() {
        Map<String, String> env = env();
        env.put("STATE_RECOVERY_PATH", "   ");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsInvalidReplayBoolean() {
        Map<String, String> env = env();
        env.put("ALLOW_FULL_REPLAY", "yes");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
        env.put("ALLOW_FULL_REPLAY", "");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void defaultsCandleCurrentTableAndHonorsOverride() {
        assertEquals("feature_candles_15s_current", SignalJobConfig.from(env()).candleCurrentTable());
        Map<String, String> env = env();
        env.put("CANDLE_CURRENT_TABLE", "candles_current_dev");
        assertEquals("candles_current_dev", SignalJobConfig.from(env).candleCurrentTable());
    }

    @Test
    void trimsRestorePathWhitespace() {
        Map<String, String> env = env();
        env.remove("ALLOW_FULL_REPLAY");
        env.put("STATE_RECOVERY_PATH", "  file:///tmp/chk  ");
        assertEquals("file:///tmp/chk", SignalJobConfig.from(env).stateRecoveryPath());
    }

    @Test
    void honorsOtelCollectorHostOverride() {
        Map<String, String> env = env();
        env.put("OTEL_COLLECTOR_HOST", "localhost:4318");
        assertEquals("localhost:4318", SignalJobConfig.from(env).otelCollectorHost());
    }

    @Test
    void rejectsDedupTtlDifferentFromPinned() {
        Map<String, String> env = env();
        env.put("DEDUP_TTL_MS", "100");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsMissingDedupTtl() {
        Map<String, String> env = env();
        env.remove("DEDUP_TTL_MS");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsCandleWindowDifferentFromPinned() {
        Map<String, String> env = env();
        env.put("CANDLE_WINDOW_MS", "20000");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsCheckpointIntervalDifferentFromPinned() {
        Map<String, String> env = env();
        env.put("CHECKPOINT_INTERVAL_MS", "5000");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsMaxConcurrentCheckpointsDifferentFromPinned() {
        Map<String, String> env = env();
        env.put("MAX_CONCURRENT_CHECKPOINTS", "3");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsNonPositiveSourceIdleAlertMs() {
        // A zero/negative alert threshold would fire on every scheduler
        // pause — observability knob must stay strictly positive.
        Map<String, String> env = env();
        env.put("SOURCE_IDLE_ALERT_MS", "0");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("SOURCE_IDLE_ALERT_MS"),
                "error must name the key, got: " + e.getMessage());
        env.put("SOURCE_IDLE_ALERT_MS", "-1");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void honorsTuningOverrides() {
        Map<String, String> env = env();
        env.put("WATERMARK_OUT_OF_ORDER_MS", "2500");
        env.put("ALLOWED_LATENESS_MS", "1000");
        env.put("SOURCE_IDLE_MS", "20000");
        env.put("SOURCE_IDLE_ALERT_MS", "90000");
        env.put("RESTART_MAX_ATTEMPTS", "5");
        env.put("RESTART_DELAY_MS", "45000");
        env.put("FLUSS_BOOTSTRAP_SERVERS", "fluss:9123");
        env.put("RAW_TABLE", "raw_table_1");
        env.put("CANDLE_TABLE", "feature_candles_15s");
        env.put("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates_dev");
        env.put("SIGNAL_STRATEGY_ID", "my-strategy");
        env.put("SIGNAL_STRATEGY_VERSION", "2.1.0");
        env.put("SIGNAL_RULE_ID", "my-rule");
        env.put("SIGNAL_LOOKBACK_CANDLES", "5");
        env.put("SIGNAL_QUANTITY", "3");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals(2_500L, cfg.outOfOrderMs());
        assertEquals(1_000L, cfg.allowedLatenessMs());
        assertEquals(20_000L, cfg.sourceIdleMs());
        assertEquals(90_000L, cfg.sourceIdleAlertMs());
        assertEquals(5, cfg.restartMaxAttempts());
        assertEquals(45_000L, cfg.restartDelayMs());
        assertEquals("fluss:9123", cfg.bootstrapServers());
        assertEquals("Signal_Candidates_dev", cfg.signalCandidatesTable());
        assertEquals("my-strategy", cfg.signalStrategyId());
        assertEquals("2.1.0", cfg.signalStrategyVersion());
        assertEquals("my-rule", cfg.signalRuleId());
        assertEquals(5, cfg.signalLookbackCandles());
        assertEquals(3L, cfg.signalQuantity());
    }

    // ── tracker 14 P2: canonical version pair is pinned fail-closed
    //    (CANDLE-CANONICAL-001), NOT a tuning knob ──

    @Test
    void acceptsCanonicalVersionPair() {
        SignalJobConfig cfg = SignalJobConfig.from(env());
        assertEquals(CandleTableSchema.CANONICAL_ALGORITHM_VERSION, cfg.algorithmVersion());
        assertEquals(CandleTableSchema.CANONICAL_CONFIGURATION_VERSION, cfg.configurationVersion());
    }

    @Test
    void rejectsDeviatingAlgorithmVersion() {
        Map<String, String> env = env();
        env.put("ALGORITHM_VERSION", "candle-15s-v2");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("ALGORITHM_VERSION"),
                "error must name the deviating key, got: " + e.getMessage());
        assertTrue(e.getMessage().contains("canonical"),
                "error must cite the canonical pair, got: " + e.getMessage());
    }

    @Test
    void rejectsDeviatingConfigurationVersion() {
        Map<String, String> env = env();
        env.put("CONFIGURATION_VERSION", "2.0.0");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void missingCanonicalVersionFallsBackToCanonicalDefault() {
        // The documented default IS the canonical pair — a missing key is safe.
        Map<String, String> env = env();
        env.remove("ALGORITHM_VERSION");
        env.remove("CONFIGURATION_VERSION");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals(CandleTableSchema.CANONICAL_ALGORITHM_VERSION, cfg.algorithmVersion());
        assertEquals(CandleTableSchema.CANONICAL_CONFIGURATION_VERSION, cfg.configurationVersion());
    }

    @Test
    void rejectsBlankCanonicalVersion() {
        Map<String, String> env = env();
        env.put("CONFIGURATION_VERSION", "  ");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsLookbackBelowTwo() {
        Map<String, String> env = env();
        env.put("SIGNAL_LOOKBACK_CANDLES", "1");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    void rejectsNonPositiveQuantity() {
        Map<String, String> env = env();
        env.put("SIGNAL_QUANTITY", "0");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    // ── tracker 14 P4: state backend + durable checkpoints ────────────────

    @Test
    void devDefaultsKeepLiveRunCompatible() {
        // Default DEPLOYMENT_ENV=dev + STATE_BACKEND=hashmap keeps the live dev
        // run's HashMapStateBackend checkpoints restorable on the next restart.
        SignalJobConfig cfg = SignalJobConfig.from(env());
        assertEquals("dev", cfg.deploymentEnv());
        assertEquals("hashmap", cfg.stateBackend());
        assertEquals(1, cfg.parallelism());
        assertTrue(cfg.stateBackendManagedMemory());
        assertEquals(null, cfg.savepointDir());
    }

    @Test
    void productionDefaultsToRocksdb() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("rocksdb", cfg.stateBackend());
        assertEquals("https://signal-test.r2.cloudflarestorage.com", cfg.s3Endpoint());
        assertEquals("r2accesskey000000000000", cfg.s3AccessKey());
        assertEquals("r2s3cr3tvalue000000000000", cfg.s3SecretKey());
        assertEquals("auto", cfg.s3Region(), "R2 signs per-endpoint — default region is 'auto'");
        assertTrue(cfg.s3PathStyle(), "R2 requires path-style addressing — default is true");
    }

    @Test
    void s3ObjectStoreWithoutCredentialsFailsClosed() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("S3_ENDPOINT"), e.getMessage());
        assertTrue(e.getMessage().contains("AWS_ACCESS_KEY_ID"), e.getMessage());
        assertTrue(e.getMessage().contains("AWS_SECRET_ACCESS_KEY"), e.getMessage());
    }

    @Test
    void r2EndpointFallbackWhenS3EndpointAbsent() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("R2_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("https://signal-test.r2.cloudflarestorage.com", cfg.s3Endpoint(),
                "R2_ENDPOINT is the accepted fallback when S3_ENDPOINT is unset");
    }

    @Test
    void devLocalCheckpointNeedsNoS3Credentials() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("CHECKPOINT_DIR", "/tmp/signaljob-checkpoints");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals(null, cfg.s3Endpoint(),
                "no object-store URI → no endpoint, no credential requirement");
    }

    @Test
    void rejectsHeapStateInProduction() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("STATE_BACKEND", "hashmap");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("STATE_BACKEND=hashmap is forbidden"),
                e.getMessage());
    }

    @Test
    void rejectsLocalOnlyCheckpointPathInProduction() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "/tmp/signaljob-checkpoints");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("s3://"), e.getMessage());
    }

    @Test
    void rejectsMissingCheckpointDirInProduction() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("CHECKPOINT_DIR"), e.getMessage());
    }

    @Test
    void acceptsExplicitDevLocalMode() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("STATE_BACKEND", "hashmap");
        env.put("CHECKPOINT_DIR", "/tmp/signaljob-checkpoints");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("hashmap", cfg.stateBackend());
        assertEquals("/tmp/signaljob-checkpoints", cfg.checkpointDir());
    }

    @Test
    void productionAcceptsS3CheckpointAndSavepoint() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3a://signal-checkpoints/prod");
        env.put("SAVEPOINT_DIR", "s3://signal-savepoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        env.put("AWS_REGION", "us-east-1");
        env.put("S3_PATH_STYLE", "false");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("s3a://signal-checkpoints/prod", cfg.checkpointDir());
        assertEquals("s3://signal-savepoints/prod", cfg.savepointDir());
        assertEquals("us-east-1", cfg.s3Region(), "explicit AWS_REGION overrides the R2 'auto' default");
        assertFalse(cfg.s3PathStyle(), "explicit S3_PATH_STYLE=false opts into virtual-hosted addressing");
    }

    @Test
    void rejectsNonS3SavepointDirInProduction() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("SAVEPOINT_DIR", "/tmp/savepoints");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("SAVEPOINT_DIR"), e.getMessage());
    }

    @Test
    void rejectsUnknownStateBackend() {
        Map<String, String> env = env();
        env.put("STATE_BACKEND", "forst");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("STATE_BACKEND must be"), e.getMessage());
    }

    @Test
    void rejectsBlankStateBackend() {
        Map<String, String> env = env();
        env.put("STATE_BACKEND", "  ");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("blank"), e.getMessage());
    }

    @Test
    void rejectsBlankDeploymentEnv() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", " ");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("DEPLOYMENT_ENV"), e.getMessage());
    }

    @Test
    void rejectsInvalidManagedMemoryBoolean() {
        Map<String, String> env = env();
        env.put("STATE_BACKEND_MANAGED_MEMORY", "yes");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("STATE_BACKEND_MANAGED_MEMORY"), e.getMessage());
    }

    @Test
    void rejectsNonPositiveParallelism() {
        Map<String, String> env = env();
        env.put("PARALLELISM", "0");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("PARALLELISM"), e.getMessage());
    }

    @Test
    void honorsRocksdbDevOptions() {
        Map<String, String> env = env();
        env.put("STATE_BACKEND", "rocksdb");
        env.put("STATE_BACKEND_LOCAL_DIRS", "/data/rocksdb");
        env.put("STATE_BACKEND_MANAGED_MEMORY", "false");
        env.put("PARALLELISM", "2");
        env.put("SAVEPOINT_DIR", "file:///tmp/savepoints");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("rocksdb", cfg.stateBackend());
        assertEquals("/data/rocksdb", cfg.stateBackendLocalDirs());
        assertFalse(cfg.stateBackendManagedMemory());
        assertEquals(2, cfg.parallelism());
        assertEquals("file:///tmp/savepoints", cfg.savepointDir());
    }
}
