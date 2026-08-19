package com.trading.common.schema.projection;

import com.trading.common.model.PositionState;

/**
 * The authoritative Nautilus-computed position update (T6, CHG-045). Nautilus
 * is the only production position/PnL calculator; the projection layer
 * serializes this event into a Positions KV row and never recomputes
 * arithmetic. Carries the stable {@link #sourceSequence} that the version gate
 * compares against {@code Positions.source_version}.
 *
 * <p>Quantity invariants are validated here (never negative, open >= closed)
 * so an impossible Nautilus event is rejected as
 * {@link QuarantineReason#POSITION_VIOLATION} before it can reach the store.
 */
public record NautilusPositionEvent(
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
        long sourceSequence,
        long lastUpdateTs) {

    public NautilusPositionEvent {
        if (positionId == null || positionId.isBlank()) {
            throw new IllegalArgumentException("positionId is required");
        }
        if (state == null) {
            throw new IllegalArgumentException("state is required");
        }
        if (openQuantity < 0 || closedQuantity < 0 || openQuantity < closedQuantity) {
            throw new IllegalArgumentException(
                    "quantity invariant violated: open=" + openQuantity
                    + " closed=" + closedQuantity);
        }
        if (sourceSequence < 0) {
            throw new IllegalArgumentException("sourceSequence must be >= 0");
        }
    }
}
