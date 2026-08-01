package com.trading.ingestion.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class BrokerQuarantineTest {
    private final BridgeEventParser parser = new BridgeEventParser(new ObjectMapper());

    @Test
    void parsesAndValidatesPayloadHash() throws Exception {
        byte[] payload = "bad-frame".getBytes(StandardCharsets.UTF_8);
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        String json = "{\"record_type\":\"broker_quarantine\",\"contract_version\":2,\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"token\":3045,\"reason\":\"HASH_MISMATCH\",\"raw_payload\":\"YmFkLWZyYW1l\",\"payload_hash\":\"" + hash + "\",\"detected_ts_ms\":1700000000000}";
        BrokerQuarantine record = parser.parseQuarantine(json).orElseThrow();
        assertEquals(3045, record.token());
        assertEquals(hash, record.payloadHash());
    }

    @Test
    void rejectsHashMismatchAndLeavesTicksEmpty() throws Exception {
        String json = "{\"record_type\":\"broker_quarantine\",\"contract_version\":2,\"slot_id\":\"hft-0\",\"connection_id\":\"hft-0\",\"connection_epoch\":1,\"token\":3045,\"reason\":\"HASH_MISMATCH\",\"raw_payload\":\"YmFkLWZyYW1l\",\"payload_hash\":\"" + "0".repeat(64) + "\",\"detected_ts_ms\":1700000000000}";
        assertThrows(IllegalArgumentException.class, () -> parser.parseQuarantine(json));
        org.junit.jupiter.api.Assertions.assertTrue(parser.parseQuarantine("{\"record_type\":\"tick\"}").isEmpty());
    }
}
