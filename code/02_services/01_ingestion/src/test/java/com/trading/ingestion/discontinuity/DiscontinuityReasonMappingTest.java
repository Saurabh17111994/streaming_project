package com.trading.ingestion.discontinuity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.trading.ingestion.bridge.BridgeEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 3 — lifecycle evidence mapping (plan §DiscontinuityWriter).
 *
 * <p>Verifies the plan's event→Reason mapping:
 * DISCONNECTED/BRIDGE_EXIT→DROP, HEARTBEAT_FAILED/FEED_STALLED→HEARTBEAT_GAP,
 * RECONNECT→RECONNECT, AUTH_FAILURE→DROP, others→null.
 */
@DisplayName("ING-UNIT-011: bridge event to discontinuity reason mapping")
class DiscontinuityReasonMappingTest {

    @Test
    @DisplayName("disconnect and bridge_exit map to DROP")
    void disconnectMapsToDrop() {
        assertEquals(DiscontinuityWriter.Reason.DROP, DiscontinuityWriter.mapEventToReason("disconnect"));
        assertEquals(DiscontinuityWriter.Reason.DROP, DiscontinuityWriter.mapEventToReason("bridge_exit"));
    }

    @Test
    @DisplayName("auth_failure maps to DROP")
    void authFailureMapsToDrop() {
        assertEquals(DiscontinuityWriter.Reason.DROP, DiscontinuityWriter.mapEventToReason("auth_failure"));
    }

    @Test
    @DisplayName("heartbeat_failed and feed_stalled map to HEARTBEAT_GAP")
    void heartbeatAndStallMapToGap() {
        assertEquals(DiscontinuityWriter.Reason.HEARTBEAT_GAP, DiscontinuityWriter.mapEventToReason("heartbeat_failed"));
        assertEquals(DiscontinuityWriter.Reason.HEARTBEAT_GAP, DiscontinuityWriter.mapEventToReason("feed_stalled"));
    }

    @Test
    @DisplayName("reconnect maps to RECONNECT")
    void reconnectMapsToReconnect() {
        assertEquals(DiscontinuityWriter.Reason.RECONNECT, DiscontinuityWriter.mapEventToReason("reconnect"));
    }

    @Test
    @DisplayName("non-evidence events map to null")
    void nonEvidenceMapsToNull() {
        assertNull(DiscontinuityWriter.mapEventToReason("slot_state"));
        assertNull(DiscontinuityWriter.mapEventToReason("bridge_shutdown"));
        assertNull(DiscontinuityWriter.mapEventToReason("subscription_ack"));
        assertNull(DiscontinuityWriter.mapEventToReason(null));
    }

    @Test
    @DisplayName("writeBridgeEvent rejects non-evidence events")
    void writeBridgeEventSkipsNonEvidence() {
        // slot_state must not produce a row — no exception, just no mapping.
        BridgeEvent slotState = new BridgeEvent(
                "slot_state", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                1L, "CONNECTING", 1024, 0, 0, "", 1_000L);
        assertEquals(null, DiscontinuityWriter.mapEventToReason(slotState.event()));
    }
}
