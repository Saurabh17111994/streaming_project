package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.BrokerOrderId;

/**
 * Arrow {@code POST /order/regular} success response.
 *
 * The returned {@code orderNo} is the broker-authoritative order identity and is
 * mapped to {@link BrokerOrderId}. The platform {@code instruction_id} is never
 * echoed by Arrow; the round-trip is via {@code remarks} (= {@code client_order_ref}).
 */
public final class ArrowOrderResponse {

    private final BrokerOrderId brokerOrderId; // Arrow "orderNo"
    private final long requestTime; // epoch ms from Arrow

    public ArrowOrderResponse(BrokerOrderId brokerOrderId, long requestTime) {
        this.brokerOrderId = brokerOrderId;
        this.requestTime = requestTime;
    }

    public BrokerOrderId brokerOrderId() { return brokerOrderId; }
    public long requestTime() { return requestTime; }

    /** Parse a successful Arrow place response body (already JSON-decoded by caller). */
    public static ArrowOrderResponse fromJson(java.util.Map<String, Object> data) {
        Object no = data.get("orderNo");
        Object rt = data.get("requestTime");
        if (no == null) {
            throw new IllegalArgumentException("orderNo missing in Arrow response");
        }
        long time = (rt instanceof Number) ? ((Number) rt).longValue() : 0L;
        return new ArrowOrderResponse(new BrokerOrderId(String.valueOf(no)), time);
    }
}
