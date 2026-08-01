package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.InstrumentToken;

/** Arrow order-status vocabulary from rest-api/orders (order book / trade book). */
public final class ArrowOrderStatus {

    private ArrowOrderStatus() {}

    /** orderStatus values returned by GET /user/orders and the postback stream. */
    public enum OrderStatus {
        PENDING,
        OPEN,
        COMPLETE,
        CANCELLED,
        REJECTED;

        public static OrderStatus from(String s) {
            if (s == null) {
                throw new IllegalArgumentException("orderStatus is null");
            }
            return valueOf(s.trim().toUpperCase());
        }
    }

    /** reportType values describing the lifecycle event. */
    public enum ReportType {
        NEW_ACK("NewAck"),
        PENDING_NEW("PendingNew"),
        FILL("Fill"),
        CANCELED("Canceled"),
        REJECTED("Rejected");

        private final String wire;
        ReportType(String wire) { this.wire = wire; }

        public String wire() { return wire; }

        public static ReportType from(String s) {
            if (s == null) {
                throw new IllegalArgumentException("reportType is null");
            }
            String v = s.trim();
            for (ReportType r : values()) {
                if (r.wire.equalsIgnoreCase(v) || r.name().equalsIgnoreCase(v)) {
                    return r;
                }
            }
            throw new IllegalArgumentException("unknown reportType: " + v);
        }
    }

    /** Lifecycle event from the postback stream or order book row. */
    public static final class OrderUpdate {
        public final BrokerOrderId brokerOrderId;
        public final ClientOrderRef clientOrderRef; // Arrow "remarks"
        public final InstrumentToken instrumentToken; // Arrow "token"
        public final OrderStatus status;
        public final ReportType reportType;
        public final long fillQuantity;
        public final long averagePrice; // paise
        public final String exchangeOrderId;
        public final String rejectReason;

        public OrderUpdate(BrokerOrderId brokerOrderId, ClientOrderRef clientOrderRef,
                           InstrumentToken instrumentToken, OrderStatus status, ReportType reportType,
                           long fillQuantity, long averagePrice, String exchangeOrderId, String rejectReason) {
            this.brokerOrderId = brokerOrderId;
            this.clientOrderRef = clientOrderRef;
            this.instrumentToken = instrumentToken;
            this.status = status;
            this.reportType = reportType;
            this.fillQuantity = fillQuantity;
            this.averagePrice = averagePrice;
            this.exchangeOrderId = exchangeOrderId;
            this.rejectReason = rejectReason;
        }
    }
}
