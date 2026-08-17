package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.write.AppendTracker;
import org.junit.jupiter.api.Test;

class HealthDiagnosticsTest {
    @Test
    void diagnosticsExposeSlotHealthWithoutInstrumentLabels() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "ACTIVE", 3, 1024, 1024, 0, System.nanoTime());
        var diagnostics = probe.diagnostics();
        assertTrue(diagnostics.containsKey("slots"));
        assertEquals(Boolean.TRUE, diagnostics.get("data_ready"));
        assertFalse(diagnostics.toString().contains("token"));
        assertFalse(diagnostics.toString().contains("symbol"));
    }

    @Test
    void telemetryReadinessTracksOtlpHealth() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        assertFalse(probe.isTelemetryReady(), "telemetry not ready until collector healthy");
        probe.setOtlpHealthy(true);
        assertTrue(probe.isTelemetryReady());
        probe.setOtlpHealthy(false);
        assertFalse(probe.isTelemetryReady());
    }
}
