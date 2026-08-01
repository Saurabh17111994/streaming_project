/* Copyright (c) Trading Platform. All rights reserved. */
package com.trading.common.broker;

import com.trading.common.arrow.ArrowOrderStatus;
import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Capability-evidence scaffold for VM-BROKER-PBK-009 (Arrow postback stream,
 * {@code order-updates.arrow.trade} WebSocket JSON).
 *
 * <p>Proves the identity mapping from a broker postback to our {@code Fills}
 * table: {@code id} -> broker_order_id, {@code remarks} -> client_order_ref,
 * {@code token} -> instrument_token, and fill fields are preserved.
 * This is the Action Capture contract: it must consume the postback
 * independently of the Signal/Executor path.
 */
class ArrowOrderUpdateTest {

    @Test
    void mapsPostbackIdentityAndFills() {
        // Sample postback (abridged) for orderNo 2600090001, token 26009.
        ArrowOrderUpdate update = new ArrowOrderUpdate(
            new BrokerOrderId("2600090001"),
            new ClientOrderRef("INV20250721"),
            new InstrumentToken(26009),
            ArrowOrderStatus.OrderStatus.COMPLETE,
            ArrowOrderStatus.ReportType.FILL,
            "F1",            // fillId
            10,             // fillQuantity
            297510L,        // fillPrice paise
            1_752_539_000L, // fillTime s
            null,           // exchangeOrderId
            null);          // rejectReason

        assertThat(update.brokerOrderId()).isEqualTo(new BrokerOrderId("2600090001"));
        assertThat(update.clientOrderRef()).isEqualTo(new ClientOrderRef("INV20250721"));
        assertThat(update.instrumentToken()).isEqualTo(new InstrumentToken(26009));
        assertThat(update.reportType()).isEqualTo(ArrowOrderStatus.ReportType.FILL);
        assertThat(update.fillQuantity()).isEqualTo(10);
        assertThat(update.isFill()).isTrue();
    }

    @Test
    void newAckHasNoFillYet() {
        ArrowOrderUpdate ack = new ArrowOrderUpdate(
            new BrokerOrderId("2600090002"),
            new ClientOrderRef("INV20250722"),
            new InstrumentToken(1594),
            ArrowOrderStatus.OrderStatus.OPEN,
            ArrowOrderStatus.ReportType.NEW_ACK,
            null, 0, 0L, 0L, null, null);

        assertThat(ack.reportType()).isEqualTo(ArrowOrderStatus.ReportType.NEW_ACK);
        assertThat(ack.fillQuantity()).isEqualTo(0);
        assertThat(ack.isFill()).isFalse();
    }
}
