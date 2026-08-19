package com.trading.common.schema.execution;

/**
 * The broker bridge boundary for exactly one place request per attempt.
 *
 * <p>The {@link ExecutionCommandGate} performs a bridge round-trip only after
 * PREPARED and SUBMITTING are durably acknowledged, passes a verified
 * gate epoch + fence token, and classifies the outcome. The bridge never
 * decides anything about attempts or the gate — it only performs the network
 * call and reports a verifiable {@link BridgeOutcome}. Any transport exception
 * or ambiguous response becomes {@code UNKNOWN} (never success or rejection by
 * assumption — order-safety invariant).
 */
public interface BridgeCaller {

    /** A verifiable bridge place-call outcome. */
    enum OutcomeKind {
        /** Broker positively accepted the order (broker order id present). */
        ACCEPTED,
        /** Broker positively rejected the order (no live order placed). */
        REJECTED,
        /** Ambiguous: timeout, network failure, lost acknowledgement, unknown broker status. */
        UNKNOWN
    }

    /** Result of one place call. {@code UNKNOWN} never carries a broker order id. */
    record BridgeOutcome(OutcomeKind kind, String brokerOrderId, String detail) {
        public boolean ambiguous() {
            return kind == OutcomeKind.UNKNOWN;
        }
    }

    /**
     * Issue exactly one bridge place request, built from the attempt's durable
     * identity. Implementations must be pure clients (no attempt/gate state)
     * so the exactly-once guarantee lives in the caller.
     */
    BridgeOutcome call(ExecutionCommandGate.Command command);
}
