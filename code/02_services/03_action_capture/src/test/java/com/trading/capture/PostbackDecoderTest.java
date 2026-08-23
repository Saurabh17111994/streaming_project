package com.trading.capture;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PostbackDecoderTest {

    @Test
    void decodeExtractsAllFields() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("id", "BRK-1");
        raw.put("remarks", "INS123");
        raw.put("orderStatus", "COMPLETE");
        raw.put("reportType", "Fill");
        raw.put("fillShares", "2");
        raw.put("averagePrice", "15050");
        raw.put("fillPrice", "15050");
        raw.put("fillQuantity", "2");
        raw.put("fillTime", "2026-08-19T10:00:00Z");
        raw.put("token", "3045");
        raw.put("exchangeUpdateTime", "2026-08-19T10:00:00Z");
        PostbackDecoder.DecodedPostback d = PostbackDecoder.decode(raw);
        assertThat(d.brokerOrderId()).isEqualTo("BRK-1");
        assertThat(d.clientOrderRef()).isEqualTo("INS123");
        assertThat(d.orderStatus()).isEqualTo("COMPLETE");
        assertThat(d.reportType()).isEqualTo("Fill");
        assertThat(d.fillShares()).isEqualTo("2");
        assertThat(d.averagePrice()).isEqualTo("15050");
        assertThat(d.fillPrice()).isEqualTo("15050");
        assertThat(d.fillQuantity()).isEqualTo("2");
        assertThat(d.fillTime()).isEqualTo("2026-08-19T10:00:00Z");
        assertThat(d.instrumentToken()).isEqualTo("3045");
        assertThat(d.exchangeUpdateTime()).isEqualTo("2026-08-19T10:00:00Z");
        assertThat(d.receivedTsMs()).isGreaterThan(0);
    }

    @Test
    void decodeHandlesStringTrimAndNumberAndNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("id", "  BRK-1  ");
        raw.put("remarks", null);
        raw.put("fillShares", 2); // Number
        raw.put("averagePrice", 15050L);
        raw.put("token", 3045);
        raw.put("orderStatus", "  OPEN ");
        // missing reportType etc -> empty

        PostbackDecoder.DecodedPostback d = PostbackDecoder.decode(raw);
        assertThat(d.brokerOrderId()).isEqualTo("BRK-1");
        assertThat(d.clientOrderRef()).isEqualTo("");
        assertThat(d.fillShares()).isEqualTo("2");
        assertThat(d.averagePrice()).isEqualTo("15050");
        assertThat(d.instrumentToken()).isEqualTo("3045");
        assertThat(d.orderStatus()).isEqualTo("OPEN");
        assertThat(d.reportType()).isEqualTo("");
        assertThat(d.exchangeUpdateTime()).isEqualTo("");
    }

    @Test
    void decodeNullMapReturnsEmptyStrings() {
        PostbackDecoder.DecodedPostback d = PostbackDecoder.decode(null);
        assertThat(d.brokerOrderId()).isEqualTo("");
        assertThat(d.clientOrderRef()).isEqualTo("");
        assertThat(d.orderStatus()).isEqualTo("");
        assertThat(d.receivedTsMs()).isGreaterThan(0);
    }

    @Test
    void decodeDoubleNumberHandled() {
        Map<String, Object> raw = Map.of("averagePrice", 15050.5);
        PostbackDecoder.DecodedPostback d = PostbackDecoder.decode(raw);
        // toString of 15050.5 -> "15050.5"
        assertThat(d.averagePrice()).isEqualTo("15050.5");
    }
}
