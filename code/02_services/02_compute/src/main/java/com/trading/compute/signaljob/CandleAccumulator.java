package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * In-window candle accumulator — the ONLY candle state that exists.
 *
 * <p>Deliberately compact: OHLCV plus the identity columns and the
 * {@code (event_time, fingerprint)} order keys used to pick open/close
 * deterministically under out-of-order arrival and replay. No tick list, no
 * raw payloads, no fingerprint lists are ever retained (Signal dossier: "no
 * tick collection exists in active state").
 */
final class CandleAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    String exchange;
    String symbol;

    long openPaise;
    long highPaise;
    long lowPaise;
    long closePaise;
    long volume;
    long tickCount;

    /** Order key of the window's earliest event; open = its price. */
    long firstEventTime = Long.MAX_VALUE;
    String firstFingerprint;

    /** Order key of the window's latest event; close = its price. */
    long lastEventTime = Long.MIN_VALUE;
    String lastFingerprint;
}
