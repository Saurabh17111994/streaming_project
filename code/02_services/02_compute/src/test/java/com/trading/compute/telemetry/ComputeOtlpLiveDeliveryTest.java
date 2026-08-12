package com.trading.compute.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Tracker 14 P8.2 box 2/3 — LIVE delivery proof through the real OTel collector:
 * the hand-built {@link ComputeOtlpEmitter} payload (OTLP/HTTP JSON) is accepted
 * by the collector ({@code flushOnce() == 200}), which forwards it to OpenObserve
 * with its own configured credentials. The Java side proves in-JVM acceptance;
 * the OpenObserve-side landing proof (stream stats / PromQL) is captured
 * separately with the skill recipes.
 *
 * <p>No credentials leave the collector: the emitter sends no Authorization
 * header (unit-proven by {@code authFailureReturnsStatusWithoutSendingCredentials}).
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} — the
 * collector must be up ({@code docker compose up -d otel-collector} in
 * {@code code/01_platform/01_docker}); the test skips when it is not.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P8.2: live OTLP delivery through the collector")
class ComputeOtlpLiveDeliveryTest {

    @Test
    @DisplayName("collector accepts the compute metrics payload (HTTP 200)")
    void liveDeliveryThroughCollector() throws Exception {
        String host = System.getenv().getOrDefault("OTEL_COLLECTOR_HOST", "localhost:4318");
        String[] hp = host.split(":");
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 2_000);
        } catch (IOException e) {
            assumeTrue(false, "OTel collector not reachable at " + host + " — "
                    + "bring it up with: cd code/01_platform/01_docker && "
                    + "docker compose up -d otel-collector");
        }

        ComputeOtlpEmitter.recordSchemaVersionRejection();
        ComputeOtlpEmitter.recordKvFilteredNonCanonical();
        ComputeOtlpEmitter.recordDedupStateDelta(3L);
        ComputeOtlpEmitter.recordDedupExpiryIndexDelta(2L);
        ComputeOtlpEmitter.recordDedupBytesDelta(3L * 128L + 2L * 64L);

        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter(host);
        int code = emitter.flushOnce();
        assertEquals(200, code,
                "collector must accept the payload (received=" + code + ")");
        System.out.println("P8.2[live-delivery] collector=" + host + " http=" + code
                + " metric=compute.invalid.byReason.schema-version"
                + " (delta + kv-filtered + dedup gauges in the same payload)");
    }
}
