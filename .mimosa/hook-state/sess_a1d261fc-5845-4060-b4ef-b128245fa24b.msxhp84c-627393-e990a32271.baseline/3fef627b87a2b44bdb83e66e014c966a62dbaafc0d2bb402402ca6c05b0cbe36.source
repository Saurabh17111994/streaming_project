package com.trading.common.schema.position;

/**
 * Durable Positions KV store (10_positions.sql v2, PK {@code position_id}) —
 * the persistence half of the SCH-20 operator wiring. Action Capture's
 * position projector writes each applied {@link PositionSnapshot} here;
 * last-write-wins on the single-field PK (raw-client-safe, COMPAT-FLUSS-005).
 */
public interface PositionsStateStore {

    /** Current snapshot for a position id, or null when absent. */
    PositionSnapshot lookup(String positionId) throws Exception;

    /** Upserts the snapshot (full row image; last-write-wins on position_id). */
    void upsert(PositionSnapshot snapshot) throws Exception;
}
