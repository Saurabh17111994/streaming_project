package com.trading.common.identity;

/**
 * Canonical platform identity model.
 *
 * Every domain uses its own identity. An overloaded "order_id" is prohibited.
 * See docs/02_requirements/04-data.md §4.2 and DEC-007.
 */
public final class IdentityModel {

    private IdentityModel() {}

    /** Immutable platform execution request created by the Signal job. */
    public static final class InstructionId {
        private final String value;
        public InstructionId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof InstructionId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Deterministic broker-facing attempt reference (max 16 chars for Arrow remarks). */
    public static final class ClientOrderRef {
        private final String value;
        public ClientOrderRef(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ClientOrderRef that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Broker-authoritative order identity (returned by broker). */
    public static final class BrokerOrderId {
        private final String value;
        public BrokerOrderId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof BrokerOrderId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Stable instrument identity; the join key across market/postback/order books.
     *  Equals Arrow {@code Token} (int32) and {@code 01_ticks_raw.bucket.key}. */
    public static final class InstrumentToken {
        private final int value;
        public InstrumentToken(int value) { this.value = value; }
        public int value() { return value; }
        @Override public String toString() { return Integer.toString(value); }
        @Override public boolean equals(Object o) {
            return o instanceof InstrumentToken that && value == that.value;
        }
        @Override public int hashCode() { return Integer.hashCode(value); }
    }

    /** Exchange/segment qualifier (NSE, BSE, NFO, BFO, MCX, ...). */
    public static final class ExchangeId {
        private final String value;
        public ExchangeId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ExchangeId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One detected setup/audit record. */
    public static final class CandidateId {
        private final String value;
        public CandidateId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** One submission attempt. */
    public static final class ExecutionAttemptId {
        private final String value;
        public ExecutionAttemptId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** Groups entry and related position-management orders. */
    public static final class TradeContextId {
        private final String value;
        public TradeContextId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** Stable position aggregate for fills, trim, exit, re-entry. */
    public static final class PositionId {
        private final String value;
        public PositionId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** One platform-captured postback delivery. */
    public static final class PostbackEventId {
        private final String value;
        public PostbackEventId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** Broker/account isolation boundary. */
    public static final class AccountScopeId {
        private final String value;
        public AccountScopeId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** Ranking and capacity boundary. */
    public static final class PortfolioId {
        private final String value;
        public PortfolioId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** Fenced Executor ownership boundary. */
    public static final class ExecutionPartitionId {
        private final String value;
        public ExecutionPartitionId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** One portfolio capacity reservation. */
    public static final class ReservationId {
        private final String value;
        public ReservationId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** One durable safety-halt request. */
    public static final class HaltRequestId {
        private final String value;
        public HaltRequestId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }

    /** One immutable structured position action (post-MVP). */
    public static final class ActionId {
        private final String value;
        public ActionId(String value) { this.value = value; }
        public String value() { return value; }
        @Override public String toString() { return value; }
    }
}
