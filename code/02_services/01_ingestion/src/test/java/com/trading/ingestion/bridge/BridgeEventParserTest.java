package com.trading.ingestion.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class BridgeEventParserTest {
    private final BridgeEventParser parser = new BridgeEventParser(new ObjectMapper());

    @Test
    void parsesLifecycleAndLeavesTicksEmpty() throws Exception {
        String event = "{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"subscription_ack\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"state\":\"ACTIVE\",\"assigned_tokens\":2,\"acknowledged_tokens\":2,\"rejected_tokens\":0}";
        BridgeEvent parsed = parser.parse(event).orElseThrow();
        assertEquals("hft-0", parsed.slotId());
        assertTrue(parser.parse("{\"record_type\":\"tick\",\"token\":1}").isEmpty());
    }

    @Test
    void rejectsUnknownVersionAndMissingFields() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":1,\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"state\":\"ACTIVE\"}"));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"state\":\"ACTIVE\"}"));
    }

    @Test
    void rejectsUnknownEventAndInvalidEpoch() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"unknown\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"state\":\"ACTIVE\"}"));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("{\"record_type\":\"bridge_event\",\"contract_version\":2,\"event\":\"slot_state\",\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":0,\"state\":\"ACTIVE\"}"));
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
