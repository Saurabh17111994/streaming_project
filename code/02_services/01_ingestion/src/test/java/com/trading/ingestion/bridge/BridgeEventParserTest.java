package com.trading.ingestion.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class BridgeEventParserTest {
    private final BridgeEventParser parser = new BridgeEventParser(new ObjectMapper());

    /** 64 lowercase hex chars — the SHA-256 digest shape the bridge emits. */
    private static final String HEX64 =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void parsesLifecycleAndLeavesTicksEmpty() throws Exception {
        String event = "{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"subscription_ack\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"state\":\"ACTIVE\",\"assigned_tokens\":2,\"acknowledged_tokens\":2,\"rejected_tokens\":0,\"received_ts_ms\":1785700000000,\"manifest_fingerprint\":\"" + HEX64 + "\",\"assigned_token_set_hash\":\"" + HEX64 + "\"}";
        BridgeEvent parsed = parser.parse(event).orElseThrow();
        assertEquals("hft-0", parsed.slotId());
        assertEquals(HEX64, parsed.manifestFingerprint());
        assertEquals(HEX64, parsed.assignedTokenSetHash());
        assertTrue(parser.parse("{\"record_type\":\"tick\",\"token\":1}").isEmpty());
    }

    @Test
    void rejectsUnknownVersionAndMissingFields() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":1,\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"state\":\"ACTIVE\"}"));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"state\":\"ACTIVE\"}"));
        // Identity fields are required (slot-scoped safety contract).
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"slot_state\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"state\":\"ACTIVE\",\"received_ts_ms\":1785700000000}"));
        // Non-hex identity values are rejected.
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"slot_state\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"state\":\"ACTIVE\",\"received_ts_ms\":1785700000000,\"manifest_fingerprint\":\"not-a-digest\",\"assigned_token_set_hash\":\"" + HEX64 + "\"}"));
    }

    @Test
    void rejectsUnknownEventAndInvalidEpoch() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"unknown\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"state\":\"ACTIVE\",\"manifest_fingerprint\":\"" + HEX64 + "\",\"assigned_token_set_hash\":\"" + HEX64 + "\"}"));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"slot_state\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":0,\"state\":\"ACTIVE\",\"manifest_fingerprint\":\"" + HEX64 + "\",\"assigned_token_set_hash\":\"" + HEX64 + "\"}"));
    }

    @Test
    void parsesBridgeMetricsAndLeavesOthersEmpty() throws Exception {
        String metrics = "{\"record_type\":\"bridge_metrics\",\"contract_version\":2,"
                + "\"ts_ms\":1785700000000,\"reconnect_consecutive\":3,"
                + "\"active_sockets\":1,\"go_goroutines\":42}";
        BridgeMetrics parsed = parser.parseMetrics(metrics).orElseThrow();
        assertEquals(1785700000000L, parsed.tsMs());
        assertEquals(3, parsed.reconnectConsecutive());
        assertEquals(1, parsed.activeSockets());
        assertEquals(42, parsed.goGoroutines());
        // Non-metrics records fall through (never rejected).
        assertTrue(parser.parseMetrics("{\"record_type\":\"tick\",\"token\":1}").isEmpty());
        assertTrue(parser.parseMetrics("{\"record_type\":\"bridge_event\"}").isEmpty());
        // Unknown contract version on a metrics line is a hard error.
        assertThrows(IllegalArgumentException.class, () ->
                parser.parseMetrics("{\"record_type\":\"bridge_metrics\",\"contract_version\":1}"));
        // Missing/zero ts_ms is rejected.
        assertThrows(IllegalArgumentException.class, () ->
                parser.parseMetrics("{\"record_type\":\"bridge_metrics\",\"contract_version\":2,\"ts_ms\":0}"));
    }

    @Test
    void nonBridgeRecordTypesAreSkippedNotRejected() throws Exception {
        // R-028: broker_quarantine (and any other record_type) must yield
        // Optional.empty() from parse() so the caller falls through to
        // parseQuarantine() — it must NOT throw.
        byte[] raw = "{\"token\":100000}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String b64 = java.util.Base64.getEncoder().encodeToString(raw);
        String hash = java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(raw));
        String quarantine = "{\"record_type\":\"broker_quarantine\",\"contract_version\":2,"
                + "\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,"
                + "\"token\":100000,\"reason\":\"MALFORMED_JSON\",\"raw_payload\":\"" + b64 + "\","
                + "\"payload_hash\":\"" + hash + "\",\"detected_ts_ms\":1000}";
        assertTrue(parser.parse(quarantine).isEmpty(),
                "broker_quarantine must be skipped by parse()");
        assertTrue(parser.parseQuarantine(quarantine).isPresent(),
                "broker_quarantine must reach parseQuarantine()");

        // Unknown record types are skipped too — never rejected.
        assertTrue(parser.parse("{\"record_type\":\"something_else\"}").isEmpty());
    }
}
