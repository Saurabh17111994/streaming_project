package com.trading.ingestion.bridge;

import java.util.Set;

/** Validated lifecycle evidence emitted by the Go bridge. */
public record BridgeEvent(
        String event,
        int contractVersion,
        String slotId,
        String connectionId,
        long connectionEpoch,
        String state,
        int assignedTokens,
        int acknowledgedTokens,
        int rejectedTokens,
        String reason,
        long receivedTsMs) {
    public static final int CONTRACT_VERSION = 2;
    private static final Set<String> STATES = Set.of("AUTHENTICATING", "CONNECTING", "SUBSCRIBING", "ACTIVE", "STALLED", "BACKOFF", "PARTIAL", "AUTH_FAILED", "TERMINAL");
    private static final Set<String> EVENTS = Set.of("slot_state", "subscription_ack", "heartbeat_failed", "feed_stalled", "disconnect", "reconnect", "auth_failure", "bridge_shutdown");

    public BridgeEvent {
        if (contractVersion != CONTRACT_VERSION) throw new IllegalArgumentException("unsupported bridge contract version: " + contractVersion);
        if (event == null || !EVENTS.contains(event)) throw new IllegalArgumentException("unknown bridge event: " + event);
        if (slotId == null || slotId.isBlank()) throw new IllegalArgumentException("slot_id is required");
        if (connectionId == null || connectionId.isBlank()) throw new IllegalArgumentException("connection_id is required");
        if (state == null || !STATES.contains(state)) throw new IllegalArgumentException("unknown bridge state: " + state);
        if (assignedTokens < 0 || acknowledgedTokens < 0 || rejectedTokens < 0) throw new IllegalArgumentException("token counts must be non-negative");
        if (connectionEpoch <= 0) throw new IllegalArgumentException("connection_epoch must be positive");
        if (reason != null && reason.length() > 512) throw new IllegalArgumentException("reason exceeds 512 characters");
    }
}
