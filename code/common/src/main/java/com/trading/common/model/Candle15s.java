package com.trading.common.model;

/**
 * Final 15-second OHLCV candle.
 * All prices in integer paise (₹1 = 100 paise).
 * Emitted once per non-empty window. No correction rows in MVP.
 */
public record Candle15s(
    long instrumentToken,
    long windowStart,
    long windowEnd,
    long openPaise,
    long highPaise,
    long lowPaise,
    long closePaise,
    long volume,
    long tickCount,
    String candleVersion,
    long ingestTs,
    String schemaVersion
) {}
