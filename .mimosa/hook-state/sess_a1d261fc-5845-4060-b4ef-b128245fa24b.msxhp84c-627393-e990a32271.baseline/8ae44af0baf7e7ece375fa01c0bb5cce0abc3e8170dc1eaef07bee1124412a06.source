package com.trading.common.observability;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OtlpEmitterTest {

    private StructuredLogEvent sample() {
        return StructuredLogEvent.builder(
                        1_752_539_000L, "INFO", "executor", "order-submit",
                        "execution", "host-1", "vm1", "prod",
                        "executor/vm1/1.0.0/trace-1", "trace-1", "span-1",
                        "order submitted")
                .build();
    }

    @Test
    void emitAlertRoutesToTradingAlertsStream() {
        String json = OtlpEmitter.emitAlert(
                AlertThresholds.Alert.MISSING_FILL,
                "executor", "host-1", "vm1", "prod",
                "executor/vm1/1.0.0/trace-1", "Critical",
                "postback fill missing beyond SLA");
        assertThat(json).contains("\"trading_alerts\"");
        assertThat(json).contains("postback fill missing beyond SLA");
        assertThat(json).contains("\"alert.condition\"");
        assertThat(json).contains("\"alert.name\"");
        assertThat(json).contains("MISSING_FILL");
        assertThat(json).contains("\"alert.category\"");
        assertThat(json).contains("Critical");
    }

    @Test
    void emitLogProducesOtlpJsonWithRequiredIdentity() {
        String json = OtlpEmitter.emitLog(sample());
        assertThat(json).contains("\"resourceLogs\"");
        assertThat(json).contains("\"service.name\"");
        assertThat(json).contains("order submitted");
        assertThat(json).contains("\"correlation_id\"");
        assertThat(json).contains("trace-1");
    }
}
