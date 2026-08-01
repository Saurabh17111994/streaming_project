package com.trading.common.broker;

import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;

/**
 * Normalized Arrow market tick (either feed: ds.arrow.trade or socket.arrow.trade).
 *
 * The {@link InstrumentToken} is the join key to {@code 01_ticks_raw.bucket.key},
 * the order book, and the postback stream. Prices from both feeds are integer-scaled
 * (paise, x100) and must be divided by 100 for display.
 */
public final class ArrowMarketTick {

    public enum Mode { LTP, LTPC, QUOTE, FULL }

    private final ExchangeId exchange;
    private final InstrumentToken instrumentToken; // Arrow "Token"
    private final Mode mode;
    private final long exchangeTimestamp; // standard: epoch s; HFT: epoch ns
    private final long lastTradedPrice; // paise
    private final long lastTradedQty;
    private final long volume;
    private final long averagePrice; // paise
    private final long openInterest;

    public ArrowMarketTick(ExchangeId exchange, InstrumentToken instrumentToken, Mode mode,
                           long exchangeTimestamp, long lastTradedPrice, long lastTradedQty,
                           long volume, long averagePrice, long openInterest) {
        this.exchange = exchange;
        this.instrumentToken = instrumentToken;
        this.mode = mode;
        this.exchangeTimestamp = exchangeTimestamp;
        this.lastTradedPrice = lastTradedPrice;
        this.lastTradedQty = lastTradedQty;
        this.volume = volume;
        this.averagePrice = averagePrice;
        this.openInterest = openInterest;
    }

    public ExchangeId exchange() { return exchange; }
    public InstrumentToken instrumentToken() { return instrumentToken; }
    public Mode mode() { return mode; }
    public long exchangeTimestamp() { return exchangeTimestamp; }
    public long lastTradedPrice() { return lastTradedPrice; }
    public long lastTradedQty() { return lastTradedQty; }
    public long volume() { return volume; }
    public long averagePrice() { return averagePrice; }
    public long openInterest() { return openInterest; }

    /** Price in rupees from the paise-scaled wire value. */
    public double priceInRupees() { return lastTradedPrice / 100.0; }
}
