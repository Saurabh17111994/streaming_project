package com.trading.ingestion.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable suspected-discontinuity record.
 * Created on connection interruption, heartbeat timeout, decode failure bursts, etc.
 * Never fabricates exact missing sequence ranges.
 */
public final class DiscontinuityEvent {
    private final String discontinuityId;
    private final String connectionId;
    private final long connectionEpoch;
    private final String reasonCode;
    private final String reasonDescription;
    private final String evidenceSummary;
    private final String affectedScope;     // instrument segment or "*"
    private final Instant detectionTime;
    private final String status;            // OPEN, ACKNOWLEDGED, CLOSED

    private DiscontinuityEvent(Builder b) {
        this.discontinuityId = b.discontinuityId != null ? b.discontinuityId : UUID.randomUUID().toString();
        this.connectionId = b.connectionId != null ? b.connectionId : "";
        this.connectionEpoch = b.connectionEpoch;
        this.reasonCode = b.reasonCode != null ? b.reasonCode : "UNKNOWN";
        this.reasonDescription = b.reasonDescription != null ? b.reasonDescription : "";
        this.evidenceSummary = b.evidenceSummary != null ? b.evidenceSummary : "";
        this.affectedScope = b.affectedScope != null ? b.affectedScope : "*";
        this.detectionTime = b.detectionTime != null ? b.detectionTime : Instant.now();
        this.status = b.status != null ? b.status : "OPEN";
    }

    public String discontinuityId() { return discontinuityId; }
    public String connectionId() { return connectionId; }
    public long connectionEpoch() { return connectionEpoch; }
    public String reasonCode() { return reasonCode; }
    public String reasonDescription() { return reasonDescription; }
    public String evidenceSummary() { return evidenceSummary; }
    public String affectedScope() { return affectedScope; }
    public Instant detectionTime() { return detectionTime; }
    public String status() { return status; }

    @Override
    public String toString() {
        return "DiscontinuityEvent{id=" + discontinuityId.substring(0, Math.min(8, discontinuityId.length()))
                + ", reason=" + reasonCode + ", scope=" + affectedScope + "}";
    }

    public static class Builder {
        String discontinuityId;
        String connectionId;
        long connectionEpoch;
        String reasonCode;
        String reasonDescription;
        String evidenceSummary;
        String affectedScope;
        Instant detectionTime;
        String status;

        public Builder discontinuityId(String v) { this.discontinuityId = v; return this; }
        public Builder connectionId(String v) { this.connectionId = v; return this; }
        public Builder connectionEpoch(long v) { this.connectionEpoch = v; return this; }
        public Builder reasonCode(String v) { this.reasonCode = v; return this; }
        public Builder reasonDescription(String v) { this.reasonDescription = v; return this; }
        public Builder evidenceSummary(String v) { this.evidenceSummary = v; return this; }
        public Builder affectedScope(String v) { this.affectedScope = v; return this; }
        public Builder detectionTime(Instant v) { this.detectionTime = v; return this; }
        public Builder status(String v) { this.status = v; return this; }

        public DiscontinuityEvent build() { return new DiscontinuityEvent(this); }
    }
}
