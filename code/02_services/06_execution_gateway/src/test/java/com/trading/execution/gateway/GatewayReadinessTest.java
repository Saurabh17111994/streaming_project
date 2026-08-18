package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class GatewayReadinessTest {
    @Test void healthDoesNotImplyExecutionReadiness() {
        GatewayReadiness r = new GatewayReadiness();
        assertThat(r.snapshot().healthy()).isTrue();
        assertThat(r.snapshot().executionReady()).isFalse();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.fail("disconnect");
        assertThat(r.snapshot().executionReady()).isFalse();
    }
}
