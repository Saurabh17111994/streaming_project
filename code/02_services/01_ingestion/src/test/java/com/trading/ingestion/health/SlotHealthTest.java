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
}
