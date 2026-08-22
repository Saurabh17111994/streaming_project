package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Streaming-3000 T6 (decision 24): the five OHLC invariant checks, in
 * deterministic task order — HIGH, LOW, VOLUME, WINDOW_SPAN, TICK_COUNT.
 * Pure logic, no operator wiring.
 */
@DisplayName("CandleInvariantCheck: five OHLC invariants (streaming-3000 T6)")
class CandleInvariantCheckTest {

    private static final long WINDOW_MS = 15_000L;
    private static final long T0 = 1_749_999_990_000L;

    /** A valid window: high >= max(open, close), low <= min, volume >= 0, tick_count > 0. */
    private static CandleAccumulator validAcc() {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = 100;
        acc.highPaise = 120;
        acc.lowPaise = 90;
        acc.closePaise = 110;
        acc.volume = 5;
        acc.tickCount = 3;
        return acc;
    }

    private static TimeWindow window() {
        return new TimeWindow(T0, T0 + WINDOW_MS);
    }

    @Test
    @DisplayName("a valid candle passes all five checks")
    void validCandlePasses() {
        assertNull(CandleInvariantCheck.firstViolation(validAcc(), window(), WINDOW_MS),
                "a consistent candle violates nothing");
    }

    @Test
    @DisplayName("check 1: HIGH — high < max(open, close) is the first violation")
    void highBelowMaxOpenCloseFails() {
        CandleAccumulator acc = validAcc();
        acc.highPaise = 105; // below close 110
        assertEquals(CandleInvariantCheck.Reason.HIGH,
                CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));

        // High must dominate BOTH open and close: below open fails too.
        CandleAccumulator acc2 = validAcc();
        acc2.openPaise = 130;
        acc2.highPaise = 120;
        assertEquals(CandleInvariantCheck.Reason.HIGH,
                CandleInvariantCheck.firstViolation(acc2, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("check 1 boundary: high == max(open, close) passes")
    void highEqualToMaxPasses() {
        CandleAccumulator acc = validAcc();
        acc.highPaise = 110; // equals close
        assertNull(CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("check 2: LOW — low > min(open, close) fails")
    void lowAboveMinOpenCloseFails() {
        CandleAccumulator acc = validAcc();
        acc.lowPaise = 105; // > min(open=100, close=110)
        assertEquals(CandleInvariantCheck.Reason.LOW,
                CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("check 2 boundary: low == min(open, close) passes")
    void lowEqualToMinPasses() {
        CandleAccumulator acc = validAcc();
        acc.lowPaise = 100; // equals open
        assertNull(CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("check 3: negative volume fails (long overflow / corruption)")
    void negativeVolumeFails() {
        CandleAccumulator acc = validAcc();
        acc.volume = -2L;
        assertEquals(CandleInvariantCheck.Reason.VOLUME,
                CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));

        acc.volume = 0L; // zero volume is valid (window with no trades)
        assertNull(CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("check 4: window span must equal the configured candle window exactly")
    void windowSpanMustBeExact() {
        TimeWindow shortWindow = new TimeWindow(T0, T0 + WINDOW_MS - 1L);
        assertEquals(CandleInvariantCheck.Reason.WINDOW_SPAN,
                CandleInvariantCheck.firstViolation(validAcc(), shortWindow, WINDOW_MS));

        TimeWindow longWindow = new TimeWindow(T0, T0 + WINDOW_MS + 1L);
        assertEquals(CandleInvariantCheck.Reason.WINDOW_SPAN,
                CandleInvariantCheck.firstViolation(validAcc(), longWindow, WINDOW_MS));
    }

    @Test
    @DisplayName("check 5: tick_count > 0 whenever volume > 0")
    void volumeWithoutTicksFails() {
        CandleAccumulator acc = validAcc();
        acc.tickCount = 0;
        assertEquals(CandleInvariantCheck.Reason.TICK_COUNT,
                CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));

        // Zero volume with zero ticks is fine (empty-of-trades window).
        acc.volume = 0L;
        assertNull(CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("first failing check wins (deterministic task order)")
    void firstFailureWinsInTaskOrder() {
        // HIGH and LOW both broken: HIGH (task order 1) is the reason.
        CandleAccumulator acc = validAcc();
        acc.highPaise = 50;  // below open 100
        acc.lowPaise = 200;  // above close 110
        assertEquals(CandleInvariantCheck.Reason.HIGH,
                CandleInvariantCheck.firstViolation(acc, window(), WINDOW_MS));

        // Every later check broken but HIGH/LOW/VOLUME clean: TICK_COUNT (5) wins over
        // nothing earlier, proving VOLUME (3) is reported before WINDOW_SPAN (4),
        // and both before TICK_COUNT (5).
        CandleAccumulator acc2 = validAcc();
        acc2.tickCount = 0; // volume 5 > 0 → check 5
        assertEquals(CandleInvariantCheck.Reason.TICK_COUNT,
                CandleInvariantCheck.firstViolation(acc2, window(), WINDOW_MS));
    }

    @Test
    @DisplayName("reason vocabulary: quarantine codes and metric suffixes are stable")
    void reasonVocabulary() {
        assertEquals("INVALID_CANDLE_HIGH",
                CandleInvariantCheck.Reason.HIGH.quarantineCode());
        assertEquals("compute.candles.invalid.high",
                "compute.candles.invalid." + CandleInvariantCheck.Reason.HIGH.metricName());
        assertEquals(CandleInvariantCheck.Reason.WINDOW_SPAN,
                CandleInvariantCheck.Reason.fromQuarantineCode("INVALID_CANDLE_WINDOW_SPAN"));
        assertNull(CandleInvariantCheck.Reason.fromQuarantineCode("MALFORMED_JSON"),
                "ingestion-side reasons are not candle reasons");
    }
}
