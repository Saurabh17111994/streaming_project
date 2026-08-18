package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class GatewayProtocolTest {
    @Test void roundTripAuthenticatesAndChecksPayloadHash() throws Exception {
        GatewayProtocol p = new GatewayProtocol("test-secret");
        var payload = new ObjectMapper().createObjectNode().put("instruction_id", "i-1");
        var e = new GatewayProtocol.Envelope("execution-gateway.v1", "EXECUTION_EVENT", "r-1",
                "acct", "part", GatewayProtocol.sha256(new ObjectMapper().writeValueAsBytes(payload)),
                4, "fence-4", System.currentTimeMillis() + 10_000, payload, null);
        String json = p.encode(e);
        assertThat(p.verify(json, "execution-gateway.v1", System.currentTimeMillis()).accepted()).isTrue();
        assertThat(p.verify(json.replace("fence-4", "fence-5"), "execution-gateway.v1",
                System.currentTimeMillis()).reason()).isEqualTo("authentication failed");
    }

    @Test void rejectsExpiredAndWrongVersionEnvelopes() throws Exception {
        GatewayProtocol p = new GatewayProtocol("secret");
        var payload = new ObjectMapper().createObjectNode().put("x", 1);
        var e = new GatewayProtocol.Envelope("v1", "X", "r", "a", "p",
                GatewayProtocol.sha256(new ObjectMapper().writeValueAsBytes(payload)), 1, "f", 10, payload, null);
        String json = p.encode(e);
        assertThat(p.verify(json, "v2", 0).reason()).isEqualTo("unsupported version");
        assertThat(p.verify(json, "v1", 11).reason()).isEqualTo("deadline expired");
    }
}
