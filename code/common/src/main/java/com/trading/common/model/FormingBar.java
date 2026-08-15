package com.trading.common.model;

/**
 * Forming bar state — passed in-process from Compute to Business Logic.
 * Updated on every eligible trade within a forming window.
 * All prices in integer paise (₹1 = 100 paise).
 *
 * <p>This is the LIVE, mutable forming-candle state — NOT a historical
 * artifact. The finalized 15-second candle is a separate object produced by
 * the completed-candle pipeline. When durable persistence is implemented for
 * the forming bar it SHALL use KV/current-state/upsert semantics only (the
 * latest forming state replaces the previous state per instrument/window);
 * per-tick snapshots are transient in-process events and SHALL NOT be
 * retained as historical records.
 *
 * <p>{@code exchange} + {@code symbol} are the source metadata required by
 * REQ-FC-007/AC-FC-014 ("source metadata" in the typed in-job event). They
 * are in-process event fields: the {@code forming_bar} KV projection (v1, 11
 * columns) deliberately does not persist them — rehydration restores them
 * from the completed-candle stream.
 */
public record FormingBar(
    long instrumentToken,
    long windowStart,
    long windowEnd,
    long openPaise,
    long highPaise,
    long lowPaise,
    long closePaise,
    long volume,
    long tickCount,
    long lastEventTime,
    String lastFingerprint,
    String exchange,
    String symbol
) {}
