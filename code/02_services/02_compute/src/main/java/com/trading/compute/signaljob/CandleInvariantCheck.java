package com.trading.compute.signaljob;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * OHLC invariant gate for finalized candles (streaming-3000 hardening plan
 * decision 24 — "OHLC checks: 5 invariants → quarantine"; T6). Pure,
 * side-effect-free check logic — {@link CandleEmitFunction} runs it on every
 * window emission before anything is handed downstream; a violating candle is
 * quarantined (never emitted, never forwarded to signal detection).
 *
 * <p>The five machine checks (task order — the FIRST failing check is the
 * quarantine reason, so the classification is deterministic):
 * <ol>
 *   <li>{@link Reason#HIGH} — {@code high >= max(open, close)}</li>
 *   <li>{@link Reason#LOW} — {@code low <= min(open, close)}</li>
 *   <li>{@link Reason#VOLUME} — {@code volume >= 0} (a negative volume can
 *       only arise from accumulator corruption or long overflow)</li>
 *   <li>{@link Reason#WINDOW_SPAN} — the window spans exactly one configured
 *       candle window ({@code window_end - window_start == candleWindowMs};
 *       15 000 ms in the pinned config)</li>
 *   <li>{@link Reason#TICK_COUNT} — {@code tick_count > 0} whenever
 *       {@code volume > 0} (aggregation increments tick_count only on qty&gt;0
 *       TRADE rows, so volume without ticks implies corruption)</li>
 * </ol>
 *
 * <p>Valid aggregation cannot violate 1, 2 or 5 (the window operator builds
 * them by construction); the gate is defense-in-depth against accumulator
 * corruption, long overflow, and window-assignment drift — and the quarantine
 * evidence preserves the exact offending row for repair.
 */
final class CandleInvariantCheck {

    private CandleInvariantCheck() {}

    /** The five invariant checks, in deterministic (task) order. */
    enum Reason {
        HIGH("INVALID_CANDLE_HIGH", "high"),
        LOW("INVALID_CANDLE_LOW", "low"),
        VOLUME("INVALID_CANDLE_VOLUME", "volume"),
        WINDOW_SPAN("INVALID_CANDLE_WINDOW_SPAN", "window_span"),
        TICK_COUNT("INVALID_CANDLE_TICK_COUNT", "tick_count");

        private final String quarantineCode;
        private final String metricName;

        Reason(String quarantineCode, String metricName) {
            this.quarantineCode = quarantineCode;
            this.metricName = metricName;
        }

        /** The {@code reason} column value written to {@code ingestion_quarantine}. */
        String quarantineCode() {
            return quarantineCode;
        }

        /** The {@code compute.candles.invalid.*} counter suffix (lowercase snake). */
        String metricName() {
            return metricName;
        }

        /** Reverse lookup for the quarantine counter consumer; {@code null} for unknown codes. */
        static Reason fromQuarantineCode(String code) {
            for (Reason r : values()) {
                if (r.quarantineCode.equals(code)) {
                    return r;
                }
            }
            return null;
        }
    }

    /**
     * First failing check for the window's accumulator, in task order, or
     * {@code null} when the candle passes all five. {@code candleWindowMs}
     * is the configured window size the DDL contract pins (15000).
     */
    static Reason firstViolation(CandleAccumulator acc, TimeWindow window, long candleWindowMs) {
        if (acc.highPaise < Math.max(acc.openPaise, acc.closePaise)) {
            return Reason.HIGH;
        }
        if (acc.lowPaise > Math.min(acc.openPaise, acc.closePaise)) {
            return Reason.LOW;
        }
        if (acc.volume < 0) {
            return Reason.VOLUME;
        }
        if (window.getEnd() - window.getStart() != candleWindowMs) {
            return Reason.WINDOW_SPAN;
        }
        if (acc.volume > 0 && acc.tickCount <= 0) {
            return Reason.TICK_COUNT;
        }
        return null;
    }
}
