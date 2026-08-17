package com.trading.ingestion.discontinuity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.bridge.BridgeEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-FAIL-001: disconnect/reconnect broker (plan §IngestionService, §DiscontinuityWriter).
 *
 * <p>The pass result is "connection epoch increases and subscription completeness is
 * rechecked". This test pins the evidence-layer contract of that transition:
 *
 * <ul>
 *   <li>{@code disconnect} at epoch N carries DROP evidence with epoch N;</li>
 *   <li>{@code reconnect} at epoch N+1 carries RECONNECT evidence with epoch N+1 —
 *       epochs strictly increase across the sequence;</li>
 *   <li>a full {@code subscription_ack} (rejected == 0) after reconnect carries no
 *       evidence — the completeness recheck confirms the subscription, it does not
 *       create a second discontinuity row.</li>
 * </ul>
 *
 * <p>The service-side epoch increment on bridge exit and the readiness/subscription
 * completeness recheck are covered by {@code BridgeRestartDecisionTest}
 * (ING-UNIT-012), the health tests ({@code ReadinessRecoveryTest},
 * {@code SlotHealthTest}) and live by ING-E2E-001.
 */
@DisplayName("ING-FAIL-001: disconnect/reconnect epoch sequence evidence")
class ReconnectEpochSequenceTest {

    /** 64 lowercase hex chars — the SHA-256 digest shape the bridge emits. */
    private static final String HEX64 =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    @DisplayName("disconnect at epoch N carries DROP evidence with epoch N")
    void disconnectCarriesDropEvidenceWithEpoch() {
        BridgeEvent disconnect = new BridgeEvent(
                "disconnect", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                7L, "ACTIVE", 1024, 0, 0, "connection reset", 1_000L, HEX64, HEX64);
        assertEquals(true, DiscontinuityWriter.carriesDiscontinuityEvidence(disconnect));
        assertEquals(DiscontinuityWriter.Reason.DROP,
                DiscontinuityWriter.mapEventToReason(disconnect.event()));
        assertEquals(7L, disconnect.connectionEpoch());
    }

    @Test
    @DisplayName("reconnect at epoch N+1 carries RECONNECT evidence — epoch increases")
    void reconnectCarriesEvidenceWithIncreasedEpoch() {
        BridgeEvent disconnect = new BridgeEvent(
                "disconnect", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                7L, "ACTIVE", 1024, 0, 0, "connection reset", 1_000L, HEX64, HEX64);
        BridgeEvent reconnect = new BridgeEvent(
                "reconnect", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                8L, "ACTIVE", 1024, 1024, 0, "", 2_000L, HEX64, HEX64);
        assertEquals(DiscontinuityWriter.Reason.DROP,
                DiscontinuityWriter.mapEventToReason(disconnect.event()));
        assertEquals(DiscontinuityWriter.Reason.RECONNECT,
                DiscontinuityWriter.mapEventToReason(reconnect.event()));
        assertTrue(reconnect.connectionEpoch() > disconnect.connectionEpoch(),
                "reconnect epoch must be strictly greater than the pre-disconnect epoch");
        assertEquals(true, DiscontinuityWriter.carriesDiscontinuityEvidence(reconnect),
                "reconnect must carry RECONNECT evidence");
    }

    @Test
    @DisplayName("full subscription_ack after reconnect carries no evidence")
    void fullSubscriptionAckAfterReconnectIsNotEvidence() {
        BridgeEvent reconnect = new BridgeEvent(
                "reconnect", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                8L, "ACTIVE", 1024, 1024, 0, "", 2_000L, HEX64, HEX64);
        BridgeEvent fullAck = new BridgeEvent(
                "subscription_ack", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                8L, "ACTIVE", 1024, 1024, 0, "", 2_100L, HEX64, HEX64);
        assertEquals(true, DiscontinuityWriter.carriesDiscontinuityEvidence(reconnect));
        assertEquals(false, DiscontinuityWriter.carriesDiscontinuityEvidence(fullAck),
                "a full ack confirms subscription completeness — it must not add a row");
    }

    @Test
    @DisplayName("partial subscription_ack after reconnect carries FEED_HEALTH evidence")
    void partialSubscriptionAckAfterReconnectIsEvidence() {
        BridgeEvent partialAck = new BridgeEvent(
                "subscription_ack", BridgeEvent.CONTRACT_VERSION, "hft-0", "ingestion-local/hft-0",
                8L, "PARTIAL", 1024, 900, 124, "tokens rejected", 2_100L, HEX64, HEX64);
        assertEquals(DiscontinuityWriter.Reason.FEED_HEALTH,
                DiscontinuityWriter.mapEventToReason(partialAck.event()));
        assertEquals(true, DiscontinuityWriter.carriesDiscontinuityEvidence(partialAck),
                "a partial ack means the completeness recheck failed — FEED_HEALTH evidence");
    }
}
