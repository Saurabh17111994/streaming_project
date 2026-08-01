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
}
