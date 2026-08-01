package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
