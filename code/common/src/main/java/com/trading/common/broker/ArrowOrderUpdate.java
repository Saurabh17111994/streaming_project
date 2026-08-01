package com.trading.common.broker;

import com.trading.common.arrow.ArrowOrderStatus;
import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.InstrumentToken;

/**
 * Arrow postback event from {@code wss://order-updates.arrow.trade}.
 *
 * This is the Action Capture source for the {@code Fills} table. Identity mapping:
 * {@code id} -> {@link BrokerOrderId}, {@code remarks} -> {@link ClientOrderRef}
 * (the platform round-trip ref, max 16 chars), {@code token} -> {@link InstrumentToken}.
 */
public final class ArrowOrderUpdate {

    private final BrokerOrderId brokerOrderId;
    private final ClientOrderRef clientOrderRef;
    private final InstrumentToken instrumentToken;
    private final ArrowOrderStatus.OrderStatus status;
    private final ArrowOrderStatus.ReportType reportType;
    private final String fillId; // trade-book fillID when reportType == FILL
    private final long fillQuantity;
    private final long fillPrice; // paise
    private final long fillTime; // epoch s
    private final String exchangeOrderId;
    private final String rejectReason;

    public ArrowOrderUpdate(BrokerOrderId brokerOrderId, ClientOrderRef clientOrderRef,
                            InstrumentToken instrumentToken, ArrowOrderStatus.OrderStatus status,
                            ArrowOrderStatus.ReportType reportType, String fillId, long fillQuantity,
                            long fillPrice, long fillTime, String exchangeOrderId, String rejectReason) {
        this.brokerOrderId = brokerOrderId;
        this.clientOrderRef = clientOrderRef;
        this.instrumentToken = instrumentToken;
        this.status = status;
        this.reportType = reportType;
        this.fillId = fillId;
        this.fillQuantity = fillQuantity;
        this.fillPrice = fillPrice;
        this.fillTime = fillTime;
        this.exchangeOrderId = exchangeOrderId;
        this.rejectReason = rejectReason;
    }

    public BrokerOrderId brokerOrderId() { return brokerOrderId; }
    public ClientOrderRef clientOrderRef() { return clientOrderRef; }
    public InstrumentToken instrumentToken() { return instrumentToken; }
    public ArrowOrderStatus.OrderStatus status() { return status; }
    public ArrowOrderStatus.ReportType reportType() { return reportType; }
    public String fillId() { return fillId; }
    public long fillQuantity() { return fillQuantity; }
    public long fillPrice() { return fillPrice; }
    public long fillTime() { return fillTime; }
    public String exchangeOrderId() { return exchangeOrderId; }
    public String rejectReason() { return rejectReason; }

    public boolean isFill() { return reportType == ArrowOrderStatus.ReportType.FILL; }
}
