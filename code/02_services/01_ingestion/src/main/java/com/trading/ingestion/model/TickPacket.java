package com.trading.ingestion.model;

import java.time.Instant;

/**
 * Fully decoded + normalized + validated tick packet ready for Fluss append.
 * All monetary values in integer paise (₹1 = 100 paise).
 * All typed fields are verified and normalized before construction.
 */
public final class TickPacket {
    // --- provenance ---
    private final RawTick raw;
    private final ValidityClassification validity;
    private final String validityReason;

    // --- routing ---
    private final long instrumentToken;
    private final String tradingSymbol;
    private final String exchange;

    // --- event time ---
    private final Instant eventTime;       // verified UTC broker timestamp
    private final Instant ingestTs;        // local time before append
    private final Instant appendAckTs;     // local time of append ack (set post-append)

    // --- trade data (verified/normalized; prices in paise) ---
    private final long lastPricePaise;
    private final long volume;
    private final double change;            // pct change — not monetary
    private final long ohlcOpenPaise;
    private final long ohlcHighPaise;
    private final long ohlcLowPaise;
    private final long ohlcClosePaise;
    private final long averagePricePaise;
    private final long openInterest;

    // --- fingerprint ---
    private final String eventFingerprint;
    private final int fingerprintVersion;

    // --- connection identity ---
    private final String connectionId;
    private final long connectionEpoch;
    private final String instanceId;

    // --- schema ---
    private final int schemaVersion;

    private TickPacket(Builder b) {
        this.raw = b.raw;
        this.validity = b.validity;
        this.validityReason = b.validityReason != null ? b.validityReason : "";
        this.instrumentToken = b.instrumentToken;
        this.tradingSymbol = b.tradingSymbol != null ? b.tradingSymbol : "";
        this.exchange = b.exchange != null ? b.exchange : "";
        this.eventTime = b.eventTime;
        this.ingestTs = b.ingestTs;
        this.appendAckTs = Instant.EPOCH;
        this.lastPricePaise = b.lastPricePaise;
        this.volume = b.volume;
        this.change = b.change;
        this.ohlcOpenPaise = b.ohlcOpenPaise;
        this.ohlcHighPaise = b.ohlcHighPaise;
        this.ohlcLowPaise = b.ohlcLowPaise;
        this.ohlcClosePaise = b.ohlcClosePaise;
        this.averagePricePaise = b.averagePricePaise;
        this.openInterest = b.openInterest;
        this.eventFingerprint = b.eventFingerprint != null ? b.eventFingerprint : "";
        this.fingerprintVersion = b.fingerprintVersion;
        this.connectionId = b.connectionId != null ? b.connectionId : "";
        this.connectionEpoch = b.connectionEpoch;
        this.instanceId = b.instanceId != null ? b.instanceId : "";
        this.schemaVersion = b.schemaVersion;
    }

    // --- accessors ---
    public RawTick raw() { return raw; }
    public ValidityClassification validity() { return validity; }
    public String validityReason() { return validityReason; }
    public long instrumentToken() { return instrumentToken; }
    public String tradingSymbol() { return tradingSymbol; }
    public String exchange() { return exchange; }
    public Instant eventTime() { return eventTime; }
    public Instant ingestTs() { return ingestTs; }
    public Instant appendAckTs() { return appendAckTs; }
    public long lastPricePaise() { return lastPricePaise; }
    public long volume() { return volume; }
    public double change() { return change; }
    public long ohlcOpenPaise() { return ohlcOpenPaise; }
    public long ohlcHighPaise() { return ohlcHighPaise; }
    public long ohlcLowPaise() { return ohlcLowPaise; }
    public long ohlcClosePaise() { return ohlcClosePaise; }
    public long averagePricePaise() { return averagePricePaise; }
    public long openInterest() { return openInterest; }
    public String eventFingerprint() { return eventFingerprint; }
    public int fingerprintVersion() { return fingerprintVersion; }
    public String connectionId() { return connectionId; }
    public long connectionEpoch() { return connectionEpoch; }
    public String instanceId() { return instanceId; }
    public int schemaVersion() { return schemaVersion; }

    public boolean isTradeEligible() {
        return validity == ValidityClassification.VALID_TRADE && eventTime != null;
    }

    @Override
    public String toString() {
        return "TickPacket{token=" + instrumentToken + ", sym=" + tradingSymbol
                + ", pricePaise=" + lastPricePaise + ", vol=" + volume
                + ", validity=" + validity + ", fp="
                + eventFingerprint.substring(0, Math.min(12, eventFingerprint.length())) + "}";
    }

    public static class Builder {
        RawTick raw;
        ValidityClassification validity = ValidityClassification.INVALID_VALUES;
        String validityReason;
        long instrumentToken;
        String tradingSymbol;
        String exchange;
        Instant eventTime = Instant.EPOCH;
        Instant ingestTs = Instant.EPOCH;
        long lastPricePaise;
        long volume;
        double change;
        long ohlcOpenPaise, ohlcHighPaise, ohlcLowPaise, ohlcClosePaise;
        long averagePricePaise;
        long openInterest;
        String eventFingerprint;
        int fingerprintVersion = 1;
        String connectionId;
        long connectionEpoch;
        String instanceId;
        int schemaVersion = 1;

        public Builder raw(RawTick v) { this.raw = v; return this; }
        public Builder validity(ValidityClassification v) { this.validity = v; return this; }
        public Builder validityReason(String v) { this.validityReason = v; return this; }
        public Builder instrumentToken(long v) { this.instrumentToken = v; return this; }
        public Builder tradingSymbol(String v) { this.tradingSymbol = v; return this; }
        public Builder exchange(String v) { this.exchange = v; return this; }
        public Builder eventTime(Instant v) { this.eventTime = v; return this; }
        public Builder ingestTs(Instant v) { this.ingestTs = v; return this; }
        public Builder lastPricePaise(long v) { this.lastPricePaise = v; return this; }
        public Builder volume(long v) { this.volume = v; return this; }
        public Builder change(double v) { this.change = v; return this; }
        public Builder ohlcOpenPaise(long v) { this.ohlcOpenPaise = v; return this; }
        public Builder ohlcHighPaise(long v) { this.ohlcHighPaise = v; return this; }
        public Builder ohlcLowPaise(long v) { this.ohlcLowPaise = v; return this; }
        public Builder ohlcClosePaise(long v) { this.ohlcClosePaise = v; return this; }
        public Builder averagePricePaise(long v) { this.averagePricePaise = v; return this; }
        public Builder openInterest(long v) { this.openInterest = v; return this; }
        public Builder eventFingerprint(String v) { this.eventFingerprint = v; return this; }
        public Builder fingerprintVersion(int v) { this.fingerprintVersion = v; return this; }
        public Builder connectionId(String v) { this.connectionId = v; return this; }
        public Builder connectionEpoch(long v) { this.connectionEpoch = v; return this; }
        public Builder instanceId(String v) { this.instanceId = v; return this; }
        public Builder schemaVersion(int v) { this.schemaVersion = v; return this; }

        public TickPacket build() { return new TickPacket(this); }
    }
}
