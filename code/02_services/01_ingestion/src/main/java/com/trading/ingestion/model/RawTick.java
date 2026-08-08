package com.trading.ingestion.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable raw tick: original wire bytes + calculated metadata.
 * Stored before fingerprinting and validity classification.
 *
 * <p>R-252: {@link Builder#build()} validates required inputs — a null
 * {@code rawPayload} previously NPE'd at {@code builder.rawPayload.clone()}
 * and a null {@code payloadHash} NPE'd in {@link #toString()}; the builder
 * also defaulted {@code receiveTime} to {@link Instant#EPOCH}, silently
 * fabricating a 1970 receive timestamp for a tick that was never stamped.
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

    /**
     * R-216: the defensive clone in {@link #rawPayload()} is correct for
     * external consumers but is called multiple times per tick on the
     * ingestion hot path (estimatedRowSize + row conversion). This returns
     * the internal array WITHOUT copying — callers MUST treat it as
     * read-only. Only the write path inside this package uses it.
     */
    public byte[] rawPayloadUnsafe() { return rawPayload; }

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
        private byte[] rawPayload;
        private String payloadHash;
        private String hashAlgorithm = "SHA-256";
        private String protocolVersion = "";
        private String decoderVersion = "0.1.0";
        private Instant receiveTime;
        private long receiveTimeNanos;

        public Builder rawPayload(byte[] v) { this.rawPayload = v; return this; }
        public Builder payloadHash(String v) { this.payloadHash = v; return this; }
        public Builder hashAlgorithm(String v) { this.hashAlgorithm = v; return this; }
        public Builder protocolVersion(String v) { this.protocolVersion = v; return this; }
        public Builder decoderVersion(String v) { this.decoderVersion = v; return this; }
        public Builder receiveTime(Instant v) { this.receiveTime = v; return this; }
        public Builder receiveTimeNanos(long v) { this.receiveTimeNanos = v; return this; }

        /**
         * R-252: fail fast with a descriptive message instead of a deferred
         * NPE inside the constructor, and require an explicit receive time —
         * a tick with {@link Instant#EPOCH} receive time is evidence that the
         * timestamp was never recorded.
         */
        public RawTick build() {
            Objects.requireNonNull(rawPayload, "rawPayload is required");
            if (rawPayload.length == 0) {
                throw new IllegalArgumentException("rawPayload must not be empty");
            }
            Objects.requireNonNull(payloadHash, "payloadHash is required");
            Objects.requireNonNull(hashAlgorithm, "hashAlgorithm is required");
            Objects.requireNonNull(protocolVersion, "protocolVersion is required");
            Objects.requireNonNull(decoderVersion, "decoderVersion is required");
            Objects.requireNonNull(receiveTime, "receiveTime is required — a RawTick must be stamped");
            if (receiveTimeNanos <= 0) {
                throw new IllegalArgumentException(
                        "receiveTimeNanos must be a positive monotonic instant, got "
                                + receiveTimeNanos);
            }
            return new RawTick(this);
        }
    }
}
