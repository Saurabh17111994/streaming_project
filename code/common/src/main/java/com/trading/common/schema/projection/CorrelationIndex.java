package com.trading.common.schema.projection;

import java.util.Optional;

/**
 * Correlation lookup registry (T6, CHG-045). Unifies the three precedence
 * sources: broker-order-id, echoed client-order-ref, and the approved
 * reconciliation result (05-execution-core.md correlation contract). A single
 * source is authoritative; multiple matches are ambiguous and go to
 * {@link Postback_Quarantine} (see {@link PostbackCorrelator}).
 */
public interface CorrelationIndex {

    /** Lookup by broker-assigned order id (highest precedence). */
    Optional<AttemptRef> byBrokerOrderId(String brokerOrderId);

    /** Lookup by the echoed client order reference (second precedence). */
    Optional<AttemptRef> byEchoedClientOrderRef(String clientOrderRef);

    /**
     * Approved reconciliation result (lowest precedence) — only consulted when
     * the order produced no broker id and no echoed client ref.
     */
    Optional<AttemptRef> approvedReconciliation(String accountScopeId, String brokerOrderId);
}
