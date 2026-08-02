package com.trading.common.broker;

import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import java.util.Objects;

/**
 * Normalized Arrow market tick (either feed: ds.arrow.trade or socket.arrow.trade).
 *
 * The {@link InstrumentToken} is the join key to {@code 01_ticks_raw.bucket.key},
 * the order book, and the postback stream. Prices from both feeds are integer-scaled
 * (paise, x100) and must be divided by 100 for display.
 *
 * <p>R-042: the {@link Feed} discriminator disambiguates the {@code exchangeTimestamp}
 * time unit — STANDARD feeds carry epoch <b>seconds</b>, HFT feeds carry epoch
 * <b>nanoseconds</b>. Consumers must convert using the feed, never guess.
 *
 * <p>R-073: the field set now covers every declared mode — LTPC carries a
 * previous close, QUOTE carries best bid/ask, FULL carries the OHLC snapshot.
 */
public final class ArrowMarketTick {

    /** Wire feed source. Determines the exchangeTimestamp unit (R-042). */
    public enum Feed {
        /** ds.arrow.trade — exchangeTimestamp in epoch seconds. */
        STANDARD,
        /** socket.arrow.trade — exchangeTimestamp in epoch nanoseconds. */
        HFT
    }

    public enum Mode { LTP, LTPC, QUOTE, FULL }

    private final ExchangeId exchange;
    private final InstrumentToken instrumentToken; // Arrow "Token"
    private final Feed feed;
    private final Mode mode;
    private final long exchangeTimestamp; // unit per feed (R-042)
    private final long lastTradedPrice; // paise
    private final long lastTradedQty;
    private final long volume;
    private final long averagePrice; // paise
    private final long openInterest;
    // R-073: LTPC/QUOTE/FULL fields (paise for prices).
    private final long previousClose;
    private final long bestBidPrice;
    private final long bestAskPrice;
    private final long bestBidQty;
    private final long bestAskQty;
    private final long open;
    private final long high;
    private final long low;
    private final long close;

    private ArrowMarketTick(Builder b) {
        this.exchange = b.exchange;
        this.instrumentToken = b.instrumentToken;
        this.feed = b.feed;
        this.mode = b.mode;
        this.exchangeTimestamp = b.exchangeTimestamp;
        this.lastTradedPrice = b.lastTradedPrice;
        this.lastTradedQty = b.lastTradedQty;
        this.volume = b.volume;
        this.averagePrice = b.averagePrice;
        this.openInterest = b.openInterest;
        this.previousClose = b.previousClose;
        this.bestBidPrice = b.bestBidPrice;
        this.bestAskPrice = b.bestAskPrice;
        this.bestBidQty = b.bestBidQty;
        this.bestAskQty = b.bestAskQty;
        this.open = b.open;
        this.high = b.high;
        this.low = b.low;
        this.close = b.close;
    }

    public ExchangeId exchange() { return exchange; }
    public InstrumentToken instrumentToken() { return instrumentToken; }
    public Feed feed() { return feed; }
    public Mode mode() { return mode; }
    public long exchangeTimestamp() { return exchangeTimestamp; }
    public long lastTradedPrice() { return lastTradedPrice; }
    public long lastTradedQty() { return lastTradedQty; }
    public long volume() { return volume; }
    public long averagePrice() { return averagePrice; }
    public long openInterest() { return openInterest; }
    public long previousClose() { return previousClose; }
    public long bestBidPrice() { return bestBidPrice; }
    public long bestAskPrice() { return bestAskPrice; }
    public long bestBidQty() { return bestBidQty; }
    public long bestAskQty() { return bestAskQty; }
    public long open() { return open; }
    public long high() { return high; }
    public long low() { return low; }
    public long close() { return close; }

    /** Price in rupees from the paise-scaled wire value. */
    public double priceInRupees() { return lastTradedPrice / 100.0; }

    /** R-042: exchangeTimestamp converted to epoch milliseconds using the feed's unit. */
    public long exchangeTimestampMillis() {
        return feed == Feed.HFT ? exchangeTimestamp / 1_000_000L : exchangeTimestamp * 1000L;
    }

    // R-162: value semantics for sets/maps/dedup.

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArrowMarketTick that)) return false;
        return exchangeTimestamp == that.exchangeTimestamp
                && lastTradedPrice == that.lastTradedPrice
                && lastTradedQty == that.lastTradedQty
                && volume == that.volume
                && averagePrice == that.averagePrice
                && openInterest == that.openInterest
                && previousClose == that.previousClose
                && bestBidPrice == that.bestBidPrice
                && bestAskPrice == that.bestAskPrice
                && bestBidQty == that.bestBidQty
                && bestAskQty == that.bestAskQty
                && open == that.open
                && high == that.high
                && low == that.low
                && close == that.close
                && Objects.equals(exchange, that.exchange)
                && Objects.equals(instrumentToken, that.instrumentToken)
                && feed == that.feed
                && mode == that.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchange, instrumentToken, feed, mode, exchangeTimestamp,
                lastTradedPrice, lastTradedQty, volume, averagePrice, openInterest,
                previousClose, bestBidPrice, bestAskPrice, bestBidQty, bestAskQty,
                open, high, low, close);
    }

    @Override
    public String toString() {
        return "ArrowMarketTick{feed=" + feed + ", mode=" + mode + ", token="
                + instrumentToken + ", ts=" + exchangeTimestamp + ", ltp=" + lastTradedPrice + "}";
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private ExchangeId exchange;
        private InstrumentToken instrumentToken;
        private Feed feed;
        private Mode mode;
        private long exchangeTimestamp;
        private long lastTradedPrice;
        private long lastTradedQty;
        private long volume;
        private long averagePrice;
        private long openInterest;
        private long previousClose;
        private long bestBidPrice;
        private long bestAskPrice;
        private long bestBidQty;
        private long bestAskQty;
        private long open;
        private long high;
        private long low;
        private long close;

        public Builder exchange(ExchangeId v) { this.exchange = v; return this; }
        public Builder instrumentToken(InstrumentToken v) { this.instrumentToken = v; return this; }
        public Builder feed(Feed v) { this.feed = v; return this; }
        public Builder mode(Mode v) { this.mode = v; return this; }
        public Builder exchangeTimestamp(long v) { this.exchangeTimestamp = v; return this; }
        public Builder lastTradedPrice(long v) { this.lastTradedPrice = v; return this; }
        public Builder lastTradedQty(long v) { this.lastTradedQty = v; return this; }
        public Builder volume(long v) { this.volume = v; return this; }
        public Builder averagePrice(long v) { this.averagePrice = v; return this; }
        public Builder openInterest(long v) { this.openInterest = v; return this; }
        public Builder previousClose(long v) { this.previousClose = v; return this; }
        public Builder bestBidPrice(long v) { this.bestBidPrice = v; return this; }
        public Builder bestAskPrice(long v) { this.bestAskPrice = v; return this; }
        public Builder bestBidQty(long v) { this.bestBidQty = v; return this; }
        public Builder bestAskQty(long v) { this.bestAskQty = v; return this; }
        public Builder open(long v) { this.open = v; return this; }
        public Builder high(long v) { this.high = v; return this; }
        public Builder low(long v) { this.low = v; return this; }
        public Builder close(long v) { this.close = v; return this; }

        public ArrowMarketTick build() {
            Objects.requireNonNull(exchange, "exchange");
            Objects.requireNonNull(instrumentToken, "instrumentToken");
            Objects.requireNonNull(feed, "feed");
            Objects.requireNonNull(mode, "mode");
            return new ArrowMarketTick(this);
        }
    }
}
