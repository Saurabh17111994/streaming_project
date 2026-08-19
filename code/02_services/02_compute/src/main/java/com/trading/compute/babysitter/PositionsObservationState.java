package com.trading.compute.babysitter;

import java.io.Serializable;

/**
 * Checkpointed observation metadata for one {@code position_id} (Task 7 —
 * the Babysitter is a read-only observer; it stores only the latest accepted
 * version/freshness evidence, never a re-computed position).
 *
 * <p>Deliberately a POJO (public class, no-arg constructor, public getters/
 * setters) so Flink's built-in POJO serializer serializes it into the keyed
 * {@code ValueState} with no Kryo registration and no custom
 * {@code TypeSerializer}.
 */
public final class PositionsObservationState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Latest accepted source version ({@code Positions.source_version}); -1 before any apply. */
    private long sourceVersion = -1L;
    /** Source event that produced the latest accepted version. */
    private String sourceEventId;
    /** {@code Positions.last_update_ts} of the latest accepted version. */
    private long lastUpdateTs;
    /** {@code Positions.schema_version} of the latest accepted version. */
    private String schemaVersion;

    public PositionsObservationState() {
        // no-arg constructor required for POJO serialization
    }

    public PositionsObservationState(
            long sourceVersion, String sourceEventId, long lastUpdateTs, String schemaVersion) {
        this.sourceVersion = sourceVersion;
        this.sourceEventId = sourceEventId;
        this.lastUpdateTs = lastUpdateTs;
        this.schemaVersion = schemaVersion;
    }

    public long getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(long sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public String getSourceEventId() {
        return sourceEventId;
    }

    public void setSourceEventId(String sourceEventId) {
        this.sourceEventId = sourceEventId;
    }

    public long getLastUpdateTs() {
        return lastUpdateTs;
    }

    public void setLastUpdateTs(long lastUpdateTs) {
        this.lastUpdateTs = lastUpdateTs;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }
}
