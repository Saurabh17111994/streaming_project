package com.trading.ingestion.bridge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 — packet-byte preservation (plan §Tick / §Data Flow).
 *
 * <p>Verifies {@code PayloadHashValidator}: a tick whose Base64
 * {@code raw_payload} does not hash to the bridge-provided {@code payload_hash}
 * must be rejected, while a valid packet passes through untouched.
 */
@DisplayName("ING-UNIT-010: raw_payload hash validation")
class PayloadHashValidatorTest {

    @Test
    @DisplayName("valid packet bytes pass")
    void validPayloadPasses() {
        byte[] packet = {1, 2, 3, 4, 5, 40, 0, 2, 0, 0};
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(
                Base64.getEncoder().encodeToString(packet), sha256(packet), out);
        assertArrayEquals(packet, result);
        assertEquals(PayloadHashValidator.Result.VALID, out[0]);
    }

    @Test
    @DisplayName("hash mismatch is rejected")
    void hashMismatchRejected() {
        byte[] packet = {1, 2, 3, 4, 5};
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(
                Base64.getEncoder().encodeToString(packet), sha256(new byte[]{9, 9, 9}), out);
        assertNull(result);
        assertEquals(PayloadHashValidator.Result.HASH_MISMATCH, out[0]);
    }

    @Test
    @DisplayName("missing payload_hash is rejected")
    void missingHashRejected() {
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(
                Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}), null, out);
        assertNull(result);
        assertEquals(PayloadHashValidator.Result.MALFORMED_HASH, out[0]);
    }

    @Test
    @DisplayName("non-hex payload_hash is rejected")
    void nonHexHashRejected() {
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(
                Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}), "not-a-hash", out);
        assertNull(result);
        assertEquals(PayloadHashValidator.Result.MALFORMED_HASH, out[0]);
    }

    @Test
    @DisplayName("invalid Base64 is rejected")
    void invalidBase64Rejected() {
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(
                "!!!not-base64!!!", sha256(new byte[]{1}), out);
        assertNull(result);
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD, out[0]);
    }

    @Test
    @DisplayName("empty payload is rejected")
    void emptyPayloadRejected() {
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(
                Base64.getEncoder().encodeToString(new byte[0]), sha256(new byte[0]), out);
        assertNull(result);
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD, out[0]);
    }

    @Test
    @DisplayName("missing raw_payload is rejected")
    void missingPayloadRejected() {
        PayloadHashValidator.Result[] out = new PayloadHashValidator.Result[1];
        byte[] result = PayloadHashValidator.validate(null, sha256(new byte[]{1}), out);
        assertNull(result);
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD, out[0]);
    }

    private static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
