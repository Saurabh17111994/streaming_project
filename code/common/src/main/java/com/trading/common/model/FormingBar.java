package com.trading.common.model;

/**
 * Forming bar state — passed in-process from Compute to Business Logic.
 * Updated on every eligible trade within a forming window.
 * All prices in integer paise (₹1 = 100 paise).
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
    String lastFingerprint
) {}
