package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatewayConfigTest {
    static Map<String, String> values() {
        Map<String, String> m = new HashMap<>();
        m.put("FLUSS_BOOTSTRAP", "fluss:9123"); m.put("FLUSS_DATABASE", "default");
        m.put("EXECUTION_INTENT_TABLE", "Execution_Intent"); m.put("EXECUTION_GATE_TABLE", "Execution_Gate");
        m.put("EXECUTION_ATTEMPTS_TABLE", "Execution_Attempts"); m.put("ORDER_CORRELATION_TABLE", "Order_Correlation");
        m.put("PROJECTION_LEDGER_TABLE", "Postback_Projection_Ledger"); m.put("SAFETY_HALT_TABLE", "Safety_Halt_Requests");
        m.put("GATEWAY_BIND_HOST", "127.0.0.1"); m.put("GATEWAY_BIND_PORT", "9180");
        m.put("NAUTILUS_PRIVATE_ENDPOINT", "http://127.0.0.1:9190/v1/intents");
        m.put("GATEWAY_PROTOCOL_VERSION", "execution-gateway.v1"); m.put("GATEWAY_SHARED_SECRET", "private");
        m.put("GATEWAY_REQUEST_TIMEOUT_MS", "2000"); m.put("GATEWAY_POLL_TIMEOUT_MS", "250");
        m.put("ACCOUNT_SCOPE_ID", "acct"); m.put("EXECUTION_PARTITION_ID", "part");
        return m;
    }
    @Test void acceptsOnlyPrivateFlussConfiguration() {
        GatewayConfig c = GatewayConfig.from(values());
        assertThat(c.flussBootstrap()).isEqualTo("fluss:9123");
        assertThat(c.bindPort()).isEqualTo(9180);
    }
    @Test void requiredSecretAndScopesCannotBeBlank() {
        Map<String, String> m = values(); m.put("GATEWAY_SHARED_SECRET", "");
        assertThatThrownBy(() -> GatewayConfig.from(m)).hasMessageContaining("GATEWAY_SHARED_SECRET");
    }
}
