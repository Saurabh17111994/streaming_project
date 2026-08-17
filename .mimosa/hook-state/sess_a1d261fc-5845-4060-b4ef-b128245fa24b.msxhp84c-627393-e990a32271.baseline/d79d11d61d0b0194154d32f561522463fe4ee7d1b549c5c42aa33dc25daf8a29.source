package com.trading.ingestion.bridge;

import java.util.Set;
import java.util.regex.Pattern;

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
        long receivedTsMs,
        String manifestFingerprint,
        String assignedTokenSetHash) {
    public static final int CONTRACT_VERSION = 2;
    private static final Set<String> STATES = Set.of("AUTHENTICATING", "CONNECTING", "SUBSCRIBING", "ACTIVE", "STALLED", "BACKOFF", "PARTIAL", "AUTH_FAILED", "TERMINAL");
    private static final Set<String> EVENTS = Set.of("slot_state", "subscription_ack", "heartbeat_failed", "feed_stalled", "disconnect", "reconnect", "auth_failure", "bridge_shutdown");

    /** SHA-256 hex digest shape (lowercase, 64 chars) — the Go bridge and
     *  InstrumentManifestLoader/SafetyHaltWriter both produce this. */
    private static final Pattern HEX64 = Pattern.compile("[0-9a-f]{64}");

    public BridgeEvent {
        if (contractVersion != CONTRACT_VERSION) throw new IllegalArgumentException("unsupported bridge contract version: " + contractVersion);
        if (event == null || !EVENTS.contains(event)) throw new IllegalArgumentException("unknown bridge event: " + event);
        if (slotId == null || slotId.isBlank()) throw new IllegalArgumentException("slot_id is required");
        if (connectionId == null || connectionId.isBlank()) throw new IllegalArgumentException("connection_id is required");
        if (state == null || !STATES.contains(state)) throw new IllegalArgumentException("unknown bridge state: " + state);
        if (assignedTokens < 0 || acknowledgedTokens < 0 || rejectedTokens < 0) throw new IllegalArgumentException("token counts must be non-negative");
        if (connectionEpoch <= 0) throw new IllegalArgumentException("connection_epoch must be positive");
        if (reason != null && reason.length() > 512) throw new IllegalArgumentException("reason exceeds 512 characters");
        // R-206: the Go-side bridge contract (validateBridgeEvent in ndjson.go)
        // requires received_ts_ms > 0 — mirror it here so a zero-timestamp
        // event cannot silently enter the discontinuity evidence path.
        if (receivedTsMs <= 0) throw new IllegalArgumentException("received_ts_ms must be positive");
        // Slot identity (plan §Slot-scoped safety propagation): both fields
        // are mandatory on the wire and must be SHA-256 hex digests so the
        // halt_request_id tuple and the cross-check stay unambiguous. The Go
        // bridge fills them from its identity map on every event.
        if (manifestFingerprint == null || manifestFingerprint.isBlank()) throw new IllegalArgumentException("manifest_fingerprint is required");
        if (!HEX64.matcher(manifestFingerprint).matches()) throw new IllegalArgumentException("manifest_fingerprint must be 64 lowercase hex chars");
        if (assignedTokenSetHash == null || assignedTokenSetHash.isBlank()) throw new IllegalArgumentException("assigned_token_set_hash is required");
        if (!HEX64.matcher(assignedTokenSetHash).matches()) throw new IllegalArgumentException("assigned_token_set_hash must be 64 lowercase hex chars");
    }
}
