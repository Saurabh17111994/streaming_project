package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class GatewayReadinessTest {
    @Test void startsNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).isEqualTo("starting");
    }
    @Test void executionReadyOnlyWhenAllDimensionsReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "fluss ok");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.protocol(true, "protocol ok");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.durableWrites(true, "durable ok");
        assertThat(r.snapshot().executionReady()).isTrue();
    }
    @Test void anyDimensionFalseMakesNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.fluss(false, "fluss down");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.fluss(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.protocol(false, "protocol mismatch");
        assertThat(r.snapshot().executionReady()).isFalse();
        r.protocol(true, "ok");
        r.durableWrites(false, "backlog");
        assertThat(r.snapshot().executionReady()).isFalse();
    }
    @Test void failClosesAll() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
        r.fail("changelog gap");
        GatewayReadiness.Snapshot s = r.snapshot();
        assertThat(s.healthy()).isFalse();
        assertThat(s.executionReady()).isFalse();
        assertThat(s.reason()).isEqualTo("changelog gap");
    }
    @Test void healthyNeverImpliesExecutionReady() {
        GatewayReadiness r = new GatewayReadiness();
        // healthy is true at start but executionReady must be false until all dims ready
        assertThat(r.snapshot().healthy()).isTrue();
        assertThat(r.snapshot().executionReady()).isFalse();
    }
    @Test void projectionBacklogAbovePolicyMakesNotReady() {
        // MAX_PENDING_PROJECTION_RECORDS policy: backlog > max -> durableWrites false
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok");
        int maxPending = 1000;
        int pending = 1001;
        boolean backlogOk = pending <= maxPending;
        r.durableWrites(backlogOk, backlogOk ? "ok" : "projection backlog " + pending + " > " + maxPending);
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).contains("backlog");
        // within limit -> ready
        r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isTrue();
    }
    @Test void bridgeDisconnectMakesNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(false, "bridge disconnected"); r.durableWrites(true, "ok");
        assertThat(r.snapshot().executionReady()).isFalse();
    }
    @Test void clockViolationMakesNotReady() {
        GatewayReadiness r = new GatewayReadiness();
        r.fluss(true, "ok"); r.protocol(true, "ok"); r.durableWrites(false, "clock offset 5s > 2s");
        assertThat(r.snapshot().executionReady()).isFalse();
        assertThat(r.snapshot().reason()).contains("clock");
    }
}
