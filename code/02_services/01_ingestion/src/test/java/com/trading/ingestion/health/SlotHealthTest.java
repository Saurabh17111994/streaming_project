package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.write.AppendTracker;
import org.junit.jupiter.api.Test;

class SlotHealthTest {
    @Test
    void dataReadinessRequiresFullAcknowledgementAndRecentFrame() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "PARTIAL", 1, 10, 9, 1, System.nanoTime());
        assertFalse(probe.isDataReady());
        probe.updateSlot("hft-0", "ACTIVE", 2, 10, 10, 0, System.nanoTime());
        assertTrue(probe.isDataReady());
    }

    @Test
    void resetSlotsReturnsToAuthenticatingAndZeroCoverage() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "ACTIVE", 3, 1024, 1024, 0, System.nanoTime());
        assertTrue(probe.isDataReady());
        probe.resetSlotsToAuthenticating();
        assertFalse(probe.isDataReady(), "after restart reset, slot must not be ready");
        HealthProbe.SlotHealth slot = probe.slot("hft-0");
        org.junit.jupiter.api.Assertions.assertEquals("AUTHENTICATING", slot.state);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.assigned);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.acknowledged);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.rejected);
    }

    @Test
    void frameArrivalRefreshesActiveSlotRecencyWithoutLifecycleEvents() throws Exception {
        // R-031: steady-state ticks update only the global setter; an ACTIVE
        // slot must stay data-ready well past the 15s timeout even though no
        // new lifecycle event ever arrives.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "ACTIVE", 1, 10, 10, 0, System.nanoTime());
        assertTrue(probe.isDataReady());

        // 16s of "steady-state" tick flow, refreshed only via setLastFrameReceived.
        long now = System.nanoTime();
        for (int i = 0; i < 8; i++) {
            now += java.time.Duration.ofSeconds(2).toNanos();
            probe.setLastFrameReceived(now);
        }
        assertTrue(probe.isDataReady(),
                "ACTIVE slot frame recency must be refreshed by setLastFrameReceived (R-031)");

        // Feed genuinely stops → slot goes stale again.
        probe.setLastFrameReceived(System.nanoTime());
        Thread.sleep(25);
        HealthProbe.SlotHealth slot = probe.slot("hft-0");
        slot.lastFrameNanos = System.nanoTime() - java.time.Duration.ofSeconds(16).toNanos();
        assertFalse(probe.isDataReady(), "stale slot must not be data-ready");
    }

    @Test
    void probeWithNoFramesIsNotFrameRecent() throws Exception {
        // R-178: nanoTime() origin is arbitrary — a probe that has never seen a
        // frame (0) must NOT be considered frame-recent.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        assertFalse(probe.isReady());
        // Drive all other dimensions ready except frame recency.
        probe.setFlussReady(true);
        probe.setBrokerConnected(true);
        probe.setSubscriptionComplete(true);
        probe.updateSlot("hft-0", "ACTIVE", 1, 10, 10, 0, System.nanoTime());
        assertFalse(probe.isReady(),
                "no frame ever received (0 nanos) must fail frame-recent (R-178)");
        probe.setLastFrameReceived(System.nanoTime());
        assertTrue(probe.isReady());
    }

    @Test
    void diagnosticsExposeTelemetryReadiness() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.setOtlpHealthy(true);
        assertTrue((Boolean) probe.diagnostics().get("telemetry_ready"),
                "diagnostics must surface telemetry readiness (R-251)");
        probe.setOtlpHealthy(false);
        assertFalse((Boolean) probe.diagnostics().get("telemetry_ready"));
    }
}
