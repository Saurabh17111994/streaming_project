package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.common.config.PlatformConfig;
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
    void honorsTuningOverrides() {
        Map<String, String> env = env();
        env.put("WATERMARK_OUT_OF_ORDER_MS", "2500");
        env.put("ALLOWED_LATENESS_MS", "1000");
        env.put("SOURCE_IDLE_MS", "20000");
        env.put("RESTART_MAX_ATTEMPTS", "5");
        env.put("RESTART_DELAY_MS", "45000");
        env.put("FLUSS_BOOTSTRAP_SERVERS", "fluss:9123");
        env.put("RAW_TABLE", "raw_table_1");
        env.put("CANDLE_TABLE", "feature_candles_15s");
        env.put("ALGORITHM_VERSION", "candle-15s-v2");
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
        assertEquals(5, cfg.restartMaxAttempts());
        assertEquals(45_000L, cfg.restartDelayMs());
        assertEquals("fluss:9123", cfg.bootstrapServers());
        assertEquals("candle-15s-v2", cfg.algorithmVersion());
        assertEquals("Signal_Candidates_dev", cfg.signalCandidatesTable());
        assertEquals("my-strategy", cfg.signalStrategyId());
        assertEquals("2.1.0", cfg.signalStrategyVersion());
        assertEquals("my-rule", cfg.signalRuleId());
        assertEquals(5, cfg.signalLookbackCandles());
        assertEquals(3L, cfg.signalQuantity());
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
}
