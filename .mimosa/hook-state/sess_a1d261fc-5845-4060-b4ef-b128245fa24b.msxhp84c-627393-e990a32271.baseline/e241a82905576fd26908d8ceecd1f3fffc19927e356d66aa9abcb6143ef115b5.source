package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.write.AppendTracker;
import org.junit.jupiter.api.Test;

class ReadinessRecoveryTest {
    @Test
    void globalReadinessRequiresActiveSlotAndRecoversAfterFullAck() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        long now = System.nanoTime();
        probe.setFlussReady(true);
        probe.setBrokerConnected(true);
        probe.setSubscriptionComplete(true);
        probe.setLastFrameReceived(now);

        probe.updateSlot("hft-0", "PARTIAL", 1, 1024, 1000, 24, now);
        assertFalse(probe.isReady());

        probe.updateSlot("hft-0", "ACTIVE", 2, 1024, 1024, 0, System.nanoTime());
        assertTrue(probe.isReady());
    }

    @Test
    void staleSlotMakesReadinessFalseEvenWhenGlobalFlagsRemainSet() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        long now = System.nanoTime();
        probe.setFlussReady(true);
        probe.setBrokerConnected(true);
        probe.setSubscriptionComplete(true);
        probe.setLastFrameReceived(now);
        probe.updateSlot("hft-0", "ACTIVE", 1, 1, 1, 0,
                now - java.time.Duration.ofSeconds(16).toNanos());
        assertFalse(probe.isReady());
    }
}
