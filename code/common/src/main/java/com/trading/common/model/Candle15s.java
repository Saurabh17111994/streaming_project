package com.trading.common.model;

/**
 * Final 15-second OHLCV candle.
 * All prices in integer paise (₹1 = 100 paise).
 * Emitted once per non-empty window. No correction rows in MVP.
 *
 * <p>R-012: field set mirrors the {@code feature_candles_15s} DDL v2 exactly —
 * {@code exchange}/{@code symbol} identity, {@code algorithm_version} +
 * {@code configuration_version} (replacing the single {@code candle_version}),
 * and {@code output_ts} (replacing {@code ingest_ts}, which was ambiguous with
 * the raw-tick ingestion timestamp).
 */
public record Candle15s(
    long instrumentToken,
    String exchange,
    String symbol,
    long windowStart,
    long windowEnd,
    long openPaise,
    long highPaise,
    long lowPaise,
    long closePaise,
    long volume,
    long tickCount,
    String algorithmVersion,
    String configurationVersion,
    long outputTs,
    String schemaVersion
) {}
