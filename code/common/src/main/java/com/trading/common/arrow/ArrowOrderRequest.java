package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;

/**
 * Arrow {@code POST /order/regular} request, typed to the platform identity model.
 *
 * Identity rule: the platform {@link ClientOrderRef} is sent in Arrow's
 * {@code remarks} field, which is capped at 16 characters. The constructor
 * enforces that cap so an over-long ref can never be submitted.
 */
public final class ArrowOrderRequest {

    public static final int REMARKS_MAX = 16;

    public enum TransactionType { B, S }
    public enum OrderType { LMT, MKT, SL_LMT, SL_MKT }
    public enum Product { I, C, M } // intraday, delivery, F&O
    public enum Validity { DAY, IOC }

    private final ExchangeId exchange;
    private final String symbol; // Arrow TradingSymbol, e.g. "ITC-EQ"
    private final InstrumentToken instrumentToken;
    private final long quantity;
    private final TransactionType transactionType;
    private final OrderType order;
    private final Product product;
    private final String price; // Arrow expects string; "0" for MKT
    private final Validity validity;
    private final long disclosedQty;
    private final ClientOrderRef clientOrderRef; // -> remarks (<=16)
    private final boolean mpp; // mimic-market; MKT disabled by default

    public ArrowOrderRequest(ExchangeId exchange, String symbol, InstrumentToken instrumentToken,
                             long quantity, TransactionType transactionType, OrderType order,
                             Product product, String price, Validity validity, long disclosedQty,
                             ClientOrderRef clientOrderRef, boolean mpp) {
        if (symbol == null || symbol.isEmpty()) {
            throw new IllegalArgumentException("symbol required");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
        String ref = clientOrderRef.value();
        if (ref == null || ref.length() > REMARKS_MAX) {
            throw new IllegalArgumentException(
                "clientOrderRef (remarks) must be <= " + REMARKS_MAX + " chars, got: " + ref);
        }
        this.exchange = exchange;
        this.symbol = symbol;
        this.instrumentToken = instrumentToken;
        this.quantity = quantity;
        this.transactionType = transactionType;
        this.order = order;
        this.product = product;
        this.price = price;
        this.validity = validity;
        this.disclosedQty = disclosedQty;
        this.clientOrderRef = clientOrderRef;
        this.mpp = mpp;
    }

    public ExchangeId exchange() { return exchange; }
    public String symbol() { return symbol; }
    public InstrumentToken instrumentToken() { return instrumentToken; }
    public long quantity() { return quantity; }
    public TransactionType transactionType() { return transactionType; }
    public OrderType order() { return order; }
    public Product product() { return product; }
    public String price() { return price; }
    public Validity validity() { return validity; }
    public long disclosedQty() { return disclosedQty; }
    public ClientOrderRef clientOrderRef() { return clientOrderRef; }
    public boolean mpp() { return mpp; }
}
