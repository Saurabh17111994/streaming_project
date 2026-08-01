package com.trading.ingestion.model;

import java.util.Objects;

/**
 * One entry from the versioned instrument manifest.
 * Every field must be validated, and required routing fields must be non-null.
 */
public final class Instrument {
    private final long instrumentToken;
    private final String tradingSymbol;
    private final String exchange;
    private final String segment;
    private final int lotSize;
    private final long manifestVersion;

    private Instrument(Builder builder) {
        this.instrumentToken = builder.instrumentToken;
        this.tradingSymbol = Objects.requireNonNull(builder.tradingSymbol, "tradingSymbol");
        this.exchange = Objects.requireNonNull(builder.exchange, "exchange");
        this.segment = builder.segment != null ? builder.segment : "";
        this.lotSize = builder.lotSize > 0 ? builder.lotSize : 1;
        this.manifestVersion = builder.manifestVersion;
    }

    public long instrumentToken() { return instrumentToken; }
    public String tradingSymbol() { return tradingSymbol; }
    public String exchange() { return exchange; }
    public String segment() { return segment; }
    public int lotSize() { return lotSize; }
    public long manifestVersion() { return manifestVersion; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Instrument that)) return false;
        return instrumentToken == that.instrumentToken;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(instrumentToken);
    }

    @Override
    public String toString() {
        return "Instrument{" + instrumentToken + "=" + tradingSymbol + "(" + exchange + ")}";
    }

    public static class Builder {
        private long instrumentToken;
        private String tradingSymbol;
        private String exchange;
        private String segment;
        private int lotSize = 1;
        private long manifestVersion;

        public Builder instrumentToken(long v) { this.instrumentToken = v; return this; }
        public Builder tradingSymbol(String v) { this.tradingSymbol = v; return this; }
        public Builder exchange(String v) { this.exchange = v; return this; }
        public Builder segment(String v) { this.segment = v; return this; }
        public Builder lotSize(int v) { this.lotSize = v; return this; }
        public Builder manifestVersion(long v) { this.manifestVersion = v; return this; }

        public Instrument build() {
            return new Instrument(this);
        }
    }
}
