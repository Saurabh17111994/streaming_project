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
        ArrowMarketTick tick = ArrowMarketTick.builder()
            .exchange(new ExchangeId("NSECM"))
            .instrumentToken(new InstrumentToken(26009))
            .feed(ArrowMarketTick.Feed.STANDARD)
            .mode(ArrowMarketTick.Mode.FULL)
            .exchangeTimestamp(1_752_539_000L) // epoch s on standard feed
            .lastTradedPrice(297510L)          // paise
            .lastTradedQty(10L)
            .volume(1_000L)
            .averagePrice(297500L)             // paise
            .openInterest(500L)
            .build();

        assertThat(tick.instrumentToken()).isEqualTo(new InstrumentToken(26009));
        assertThat(tick.lastTradedPrice()).isEqualTo(297510L);
        // Prices carried in paise; divide by 100 for rupees.
        assertThat(tick.priceInRupees()).isEqualTo(2975.10);
        // R-042: standard feed timestamp is epoch s -> ms conversion.
        assertThat(tick.exchangeTimestampMillis()).isEqualTo(1_752_539_000_000L);
    }

    @Test
    void bucketKeyMatchesRoutingIdentity() {
        ArrowMarketTick tick = ArrowMarketTick.builder()
            .exchange(new ExchangeId("NSECM"))
            .instrumentToken(new InstrumentToken(1594))
            .feed(ArrowMarketTick.Feed.HFT)
            .mode(ArrowMarketTick.Mode.LTPC)
            .build();
        assertThat(tick.instrumentToken().value()).isEqualTo(1594);
    }

    @Test
    void valueSemanticsForDedup() {
        // R-162: equals/hashCode over value fields so ticks dedupe correctly.
        ArrowMarketTick a = ArrowMarketTick.builder()
            .exchange(new ExchangeId("NSECM"))
            .instrumentToken(new InstrumentToken(26009))
            .feed(ArrowMarketTick.Feed.HFT)
            .mode(ArrowMarketTick.Mode.FULL)
            .exchangeTimestamp(1_752_539_000_000_000L)
            .lastTradedPrice(297510L)
            .build();
        ArrowMarketTick b = ArrowMarketTick.builder()
            .exchange(new ExchangeId("NSECM"))
            .instrumentToken(new InstrumentToken(26009))
            .feed(ArrowMarketTick.Feed.HFT)
            .mode(ArrowMarketTick.Mode.FULL)
            .exchangeTimestamp(1_752_539_000_000_000L)
            .lastTradedPrice(297510L)
            .build();
        ArrowMarketTick c = ArrowMarketTick.builder()
            .exchange(new ExchangeId("NSECM"))
            .instrumentToken(new InstrumentToken(26009))
            .feed(ArrowMarketTick.Feed.HFT)
            .mode(ArrowMarketTick.Mode.FULL)
            .exchangeTimestamp(1_752_539_000_000_001L) // different ts
            .lastTradedPrice(297510L)
            .build();
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
    }
}
