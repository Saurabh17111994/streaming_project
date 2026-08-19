package com.trading.common.schema.projection;

import com.trading.common.model.OrderLifecycleState;

/**
 * Projection snapshot mirroring the Order_Lifecycle KV layout
 * (09_order_lifecycle.sql v2, pinned by OrderLifecycleColumns). Written by the
 * T6 lifecycle projection under the {@code (account_scope_id, broker_order_id)}
 * composite key; source-version evidence is carried so replay/rebuild stays
 * deterministic and stale/conflict updates are rejected before upsert.
 */
public record OrderLifecycleSnapshot(
        String accountScopeId,
        String brokerOrderId,
        String instructionId,
        String executionAttemptId,
        String tradeContextId,
        OrderLifecycleState normalizedState,
        long cumulativeQty,
        long pendingQty,
        long averageFillPricePaise,
        String sourceEventId,
        long sourceVersion,
        long sourceEventTime,
        long lastReceiveTime,
        String correlationState,
        String schemaVersion) {

    public OrderLifecycleSnapshot {
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("accountScopeId is required");
        }
        if (brokerOrderId == null || brokerOrderId.isBlank()) {
            throw new IllegalArgumentException("brokerOrderId is required");
        }
        if (normalizedState == null) {
            throw new IllegalArgumentException("normalizedState is required");
        }
        if (cumulativeQty < 0 || pendingQty < 0) {
            throw new IllegalArgumentException("quantities must be >= 0");
        }
        if (cumulativeQty < pendingQty) {
            throw new IllegalArgumentException("pending_qty cannot exceed cumulative_qty");
        }
    }
}
