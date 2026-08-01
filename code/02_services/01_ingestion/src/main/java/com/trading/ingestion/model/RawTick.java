package com.trading.ingestion.model;

import java.time.Instant;
import java.util.Arrays;

/**
 * Immutable raw tick: original wire bytes + calculated metadata.
 * Stored before fingerprinting and validity classification.
 */
public final class RawTick {
    private final byte[] rawPayload;
    private final String payloadHash;         // SHA-256 of rawPayload
    private final String hashAlgorithm;       // "SHA-256"
    private final String protocolVersion;     // broker protocol version
    private final String decoderVersion;      // decoder implementation version
    private final Instant receiveTime;        // local receipt UTC
    private final long receiveTimeNanos;      // monotonic receive instant

    private RawTick(Builder builder) {
        this.rawPayload = builder.rawPayload.clone();
        this.payloadHash = builder.payloadHash;
        this.hashAlgorithm = builder.hashAlgorithm;
        this.protocolVersion = builder.protocolVersion;
        this.decoderVersion = builder.decoderVersion;
        this.receiveTime = builder.receiveTime;
        this.receiveTimeNanos = builder.receiveTimeNanos;
    }

    public byte[] rawPayload() { return rawPayload.clone(); }
    public String payloadHash() { return payloadHash; }
    public String hashAlgorithm() { return hashAlgorithm; }
    public String protocolVersion() { return protocolVersion; }
    public String decoderVersion() { return decoderVersion; }
    public Instant receiveTime() { return receiveTime; }
    public long receiveTimeNanos() { return receiveTimeNanos; }

    @Override
    public String toString() {
        return "RawTick{hash=" + payloadHash.substring(0, Math.min(12, payloadHash.length()))
                + ", bytes=" + rawPayload.length + ", proto=" + protocolVersion + "}";
    }

    public static class Builder {
        private byte[] rawPayload = new byte[0];
        private String payloadHash = "";
        private String hashAlgorithm = "SHA-256";
        private String protocolVersion = "";
        private String decoderVersion = "0.1.0";
        private Instant receiveTime = Instant.EPOCH;
        private long receiveTimeNanos;

        public Builder rawPayload(byte[] v) { this.rawPayload = v; return this; }
        public Builder payloadHash(String v) { this.payloadHash = v; return this; }
        public Builder hashAlgorithm(String v) { this.hashAlgorithm = v; return this; }
        public Builder protocolVersion(String v) { this.protocolVersion = v; return this; }
        public Builder decoderVersion(String v) { this.decoderVersion = v; return this; }
        public Builder receiveTime(Instant v) { this.receiveTime = v; return this; }
        public Builder receiveTimeNanos(long v) { this.receiveTimeNanos = v; return this; }

        public RawTick build() { return new RawTick(this); }
    }
}
