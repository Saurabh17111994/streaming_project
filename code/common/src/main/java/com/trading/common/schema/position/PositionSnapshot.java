package com.trading.common.schema.position;

import com.trading.common.model.PositionState;

/**
 * Immutable projection snapshot mirroring the Positions KV layout
 * (10_positions.sql v2). Quantity invariants: {@code open_quantity} and
 * {@code closed_quantity} are never negative and never cross
 * ({@code open >= closed}); the current quantity is derived
 * ({@code open - closed}, never persisted — v2 removed the derived column).
 */
public record PositionSnapshot(
        String positionId,
        String tradeContextId,
        String accountScopeId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        PositionState state,
        long openQuantity,
        long closedQuantity,
        long averageEntryPaise,
        long averageExitPaise,
        String sourceEventId,
        long sourceVersion,
        long createdTs,
        long lastUpdateTs,
        String schemaVersion) {

    public PositionSnapshot {
        if (positionId == null || positionId.isBlank()) {
            throw new IllegalArgumentException("position_id is required");
        }
        if (openQuantity < 0 || closedQuantity < 0 || openQuantity < closedQuantity) {
            throw new IllegalArgumentException("quantity invariant violated: open="
                    + openQuantity + " closed=" + closedQuantity);
        }
    }

    /** Current position size — derived, never persisted. */
    public long currentQuantity() {
        return openQuantity - closedQuantity;
    }
}
