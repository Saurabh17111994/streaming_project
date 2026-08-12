package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 5 — slot + resource metrics (plan §Monitoring / Amendment §Resource).
 *
 * <p>Verifies the recording API snapshots slot coverage and resource gauges,
 * and that the health callback feeds telemetry readiness.
 */
@DisplayName("ING-UNIT-012: slot + resource metrics")
class OtlpMetricsEmitterTest {

    @Test
    @DisplayName("slot state snapshot records coverage and capacity")
    void slotStateSnapshot() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.setSlotState("hft-0", true, 1024, 1024, 0, System.nanoTime());
            OtlpMetricsEmitter.SlotMetricState s = emitter.slotState("hft-0");
            assertNotNull(s, "slot state must be tracked");
            assertEquals(1, s.active);
            assertEquals(1024, s.assigned);
            assertEquals(1024, s.acknowledged);
            assertEquals(0, s.rejected);

            emitter.setSlotCapacityUsedPercent("hft-0", 87.5);
            assertEquals(87.5, s.capacityUsedPercent, 0.001);

            // Safety + capacity evidence (Step 5): unsafe stamp, duration
            // source, and remaining capacity.
            emitter.setSlotCapacityRemaining("hft-0", 256);
            assertEquals(256, s.capacityRemaining);
            assertEquals(0, s.safetyState, "fresh slot must be SAFE");
            assertEquals(0, s.unsafeSinceNanos);

            long now = System.nanoTime();
            emitter.setSlotSafetyState("hft-0", 1, now);
            assertEquals(1, s.safetyState);
            assertEquals(now, s.unsafeSinceNanos, "first unsafe transition stamps the clock");
            // Re-emission of the same unsafe state must NOT reset the stamp —
            // the unsafe-duration gauge keeps counting from the first transition.
            Thread.sleep(5);
            emitter.setSlotSafetyState("hft-0", 1, System.nanoTime());
            assertEquals(now, s.unsafeSinceNanos, "re-emitted unsafe state must keep the first stamp");
            // SAFE clears the stamp.
            emitter.setSlotSafetyState("hft-0", 0, 0);
            assertEquals(0, s.safetyState);
            assertEquals(0, s.unsafeSinceNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("resource gauges snapshot")
    void resourceGaugesSnapshot() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.setProcessOpenFds(12);
            emitter.setProcessFdLimit(1024);
            emitter.setProcessFdUsagePercent(1.17);
            emitter.setProcessRssBytes(123456L);
            emitter.setJvmThreadsLive(42);
            emitter.setGoGoroutines(7);
            emitter.setActiveSockets(1);
            emitter.setChildProcessAlive(true);
            emitter.setReconnectConsecutive(3);
            // no crash = gauges accepted; values verified via SlotMetricState-style access
            assertEquals(1, emitter.childProcessAlive());
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("health callback fires on flush result")
    void healthCallbackFires() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            AtomicBoolean seen = new AtomicBoolean(false);
            emitter.setHealthCallback(healthy -> {
                seen.set(true);
                // collector at 127.0.0.1:1 is unreachable → false expected
                assertEquals(false, healthy);
            });
            emitter.forceFlush();
            assertEquals(true, seen.get(), "health callback must fire on flush");
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("close() performs the final flush before marking closed (R-035)")
    void closeFlushesBeforeClosed() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        AtomicBoolean callbackFired = new AtomicBoolean(false);
        emitter.setHealthCallback(h -> callbackFired.set(true));
        emitter.recordTick(100);
        emitter.close();
        assertTrue(callbackFired.get(),
                "close() must run flush() while still open (R-035) — callback never fired");
    }

    @Test
    @DisplayName("payload is protobuf-JSON compliant (R-036)")
    void payloadIsSpecCompliant() throws Exception {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.setProcessFdUsagePercent(42.5);
            emitter.setSlotCapacityUsedPercent("hft-0", 12.75);
            emitter.setSlotCapacityRemaining("hft-0", 768);
            emitter.setSlotSafetyState("hft-0", 1, System.nanoTime());
            emitter.recordAppendLatencyMs(5);
            emitter.recordAppendLatencyMs(7);
            emitter.incrementDecodeError("Malformed\nJSON");
            String json = emitter.buildMetricsJson();

            // Must parse as valid JSON (control chars escaped — R-066).
            JsonNode root = new ObjectMapper().readTree(json);
            assertNotNull(root, "payload must be valid JSON");

            // Locate the asDouble gauges and assert they are numbers, not strings.
            JsonNode metrics = root.at("/resourceMetrics/0/scopeMetrics/0/metrics");
            for (JsonNode metric : metrics) {
                String name = metric.path("name").asText();
                JsonNode dp = metric.path("gauge").path("dataPoints").get(0);
                if (name.equals("process.fd_usage_percent")) {
                    assertTrue(dp.path("asDouble").isNumber(),
                            "asDouble must be a JSON number (R-036)");
                    assertEquals(42.5, dp.path("asDouble").asDouble(), 0.001);
                }
                if (name.equals("bridge.slot.capacity_used_percent")) {
                    assertTrue(dp.path("asDouble").isNumber(),
                            "labeled asDouble must be a JSON number (R-036)");
                    assertEquals(12.75, dp.path("asDouble").asDouble(), 0.001);
                }
                if (name.equals("bridge.slot.capacity_remaining")) {
                    assertEquals(768, dp.path("asInt").asLong(),
                            "capacity_remaining = connection limit − assigned");
                }
                if (name.equals("bridge.slot.safety_state")) {
                    assertEquals(1, dp.path("asInt").asInt(), "unsafe slot must export 1");
                }
                if (name.equals("bridge.slot.unsafe_duration_ms")) {
                    long ms = dp.path("asInt").asLong();
                    assertTrue(ms >= 0, "unsafe_duration_ms must be non-negative");
                }
            }

            // Sums carry aggregationTemporality + isMonotonic.
            boolean sumHasTemporality = false;
            for (JsonNode metric : metrics) {
                if (metric.has("sum")) {
                    assertEquals("AGGREGATION_TEMPORALITY_CUMULATIVE",
                            metric.path("sum").path("aggregationTemporality").asText(),
                            "sum must declare aggregationTemporality (R-036)");
                    assertTrue(metric.path("sum").path("isMonotonic").asBoolean(),
                            "sum must declare isMonotonic (R-036)");
                    sumHasTemporality = true;
                }
            }
            assertTrue(sumHasTemporality);

            // Histogram: explicitBounds is one shorter than bucketCounts, and
            // bucket totals reconcile with count.
            for (JsonNode metric : metrics) {
                if (metric.path("name").asText().equals("append.latency.ms")) {
                    JsonNode hist = metric.path("histogram").path("dataPoints").get(0);
                    long count = hist.path("count").asLong();
                    long[] buckets = new ObjectMapper().convertValue(
                            hist.path("bucketCounts"), long[].class);
                    double[] bounds = new ObjectMapper().convertValue(
                            hist.path("explicitBounds"), double[].class);
                    assertEquals(bounds.length + 1, buckets.length,
                            "bucketCounts must be one element longer than explicitBounds (R-036)");
                    long bucketSum = 0;
                    for (long b : buckets) bucketSum += b;
                    assertEquals(count, bucketSum, "bucket totals must reconcile with count (R-036)");
                }
            }

            // Reason breakdown is emitted (R-258) and the control character is
            // escaped so the payload remains valid JSON.
            String payload = json;
            assertTrue(payload.contains("decode.errors.by_reason"),
                    "decode-error reason breakdown must be emitted (R-258)");
            assertTrue(!payload.contains("Malformed\nJSON"),
                    "control characters must be escaped in the payload (R-066)");
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("latency ring wraps and keeps the latest window (R-179)")
    void latencyRingWraps() throws Exception {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            for (int i = 0; i < 2000; i++) {
                emitter.recordAppendLatencyMs(i);
            }
            String json = emitter.buildMetricsJson();
            // 1024-sample window of values 976..1999 → median ≈ 1487.
            assertTrue(json.contains("\"intValue\":1487") || json.contains("\"intValue\":1488"),
                    "p50 must reflect the latest wrapped window, not the first 1024 samples (R-179)");
        } finally {
            emitter.close();
        }
    }
}
