package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Candle emission contract (CANDLE-KV-REPLAY-001 A2): every emitted candle row
 * must carry the pinned algorithm/configuration/schema versions (REQ-FC-001),
 * the window identity, and the business OHLCV fields — the values the
 * {@code CanonicalCandlePolicy} and the KV projection rely on.
 */
@DisplayName("CandleEmitFunction.buildRow carries pinned versions and window identity")
class CandleEmitFunctionTest {

    private static final long T0 = 1_750_000_000_000L;

    @Test
    @DisplayName("row carries config algorithm/configuration/schema versions and window identity")
    void rowCarriesPinnedVersionsAndWindowIdentity() {
        SignalJobConfig config = SignalJobConfig.from(env());

        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = 100;
        acc.highPaise = 110;
        acc.lowPaise = 99;
        acc.closePaise = 105;
        acc.volume = 25;
        acc.tickCount = 7;

        TimeWindow window = new TimeWindow(T0, T0 + 15_000L);
        GenericRowData row = CandleEmitFunction.buildRow(2885L, acc, window, 1_750_000_010_000L, config);

        assertEquals(15, row.getArity(), "row must have the shared 15-column layout");
        assertEquals(2885L, row.getLong(CandleTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", row.getString(CandleTableColumns.EXCHANGE).toString());
        assertEquals("TEST", row.getString(CandleTableColumns.SYMBOL).toString());
        assertEquals(T0, row.getLong(CandleTableColumns.WINDOW_START));
        assertEquals(T0 + 15_000L, row.getLong(CandleTableColumns.WINDOW_END));
        assertEquals(100L, row.getLong(CandleTableColumns.OPEN_PAISE));
        assertEquals(110L, row.getLong(CandleTableColumns.HIGH_PAISE));
        assertEquals(99L, row.getLong(CandleTableColumns.LOW_PAISE));
        assertEquals(105L, row.getLong(CandleTableColumns.CLOSE_PAISE));
        assertEquals(25L, row.getLong(CandleTableColumns.VOLUME));
        assertEquals(7, row.getInt(CandleTableColumns.TICK_COUNT));

        // The version-carrying contract (CanonicalCandlePolicy input):
        assertEquals(config.algorithmVersion(),
                row.getString(CandleTableColumns.ALGORITHM_VERSION).toString());
        assertEquals(config.configurationVersion(),
                row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString());
        assertEquals(config.candleSchemaVersion(),
                row.getString(CandleTableColumns.SCHEMA_VERSION).toString());
        assertEquals(1_750_000_010_000L, row.getLong(CandleTableColumns.OUTPUT_TS));
    }

    @Test
    @DisplayName("emitted version values are canonical per the policy")
    void emittedVersionsAreCanonical() {
        SignalJobConfig config = SignalJobConfig.from(env());
        assertTrue(CanonicalCandlePolicy.isCanonical(
                config.algorithmVersion(), config.configurationVersion(),
                config.algorithmVersion(), config.configurationVersion()),
                "the versions the job emits must be exactly the canonical expected values");
    }

    /** Same 5-key baseline as SignalJobConfigTest; tuning keys take defaults. */
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
}
