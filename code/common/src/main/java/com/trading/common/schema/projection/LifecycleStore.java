package com.trading.common.schema.projection;

import java.util.Optional;

/**
 * Durable Order_Lifecycle KV store (09_order_lifecycle.sql v2, composite PK
 * {@code (account_scope_id, broker_order_id)}) — the persistence half of the
 * T6 lifecycle projection.
 */
public interface LifecycleStore {

    Optional<OrderLifecycleSnapshot> lookup(String accountScopeId, String brokerOrderId)
            throws Exception;

    void upsert(OrderLifecycleSnapshot snapshot) throws Exception;
}
