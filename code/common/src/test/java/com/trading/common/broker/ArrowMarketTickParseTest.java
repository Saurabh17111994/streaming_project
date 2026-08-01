/* Copyright (c) Trading Platform. All rights reserved. */
package com.trading.common.broker;

import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Capability-evidence scaffold for VM-BROKER-MKT-008 (Arrow market data feed).
 *
 * <p>Proves the tick contract: the numeric {@code Token} is the platform
 * {@code instrument_token} (join key / KV routing key) and prices are scaled
 * by 100 (paise). The binary decoder itself lives in the ingestion service;
 * this asserts the typed model the decoder must populate.
 */
class ArrowMarketTickParseTest {

    @Test
    void tokenIsInstrumentTokenJoinKey() {
        // Sample: NSECM RELIANCE token 26009, ltp 2975.10 -> 297510 paise.
        ArrowMarketTick tick = new ArrowMarketTick(
            new ExchangeId("NSECM"),
            new InstrumentToken(26009),
            ArrowMarketTick.Mode.FULL,
            1_752_539_000L, // exchangeTimestamp (epoch s on standard feed)
            297510L,        // lastTradedPrice (paise)
            10L,            // lastTradedQty
            1_000L,         // volume
            297500L,        // averagePrice (paise)
            500L);          // openInterest

        assertThat(tick.instrumentToken()).isEqualTo(new InstrumentToken(26009));
        assertThat(tick.lastTradedPrice()).isEqualTo(297510L);
        // Prices carried in paise; divide by 100 for rupees.
        assertThat(tick.priceInRupees()).isEqualTo(2975.10);
    }

    @Test
    void bucketKeyMatchesRoutingIdentity() {
        ArrowMarketTick tick = new ArrowMarketTick(
            null, new InstrumentToken(1594), ArrowMarketTick.Mode.LTPC,
            0, 0, 0, 0, 0, 0);
        assertThat(tick.instrumentToken().value()).isEqualTo(1594);
    }
}
