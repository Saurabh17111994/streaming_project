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
 *
 * <p>R-248: the result is now returned directly (no caller-owned out-array).
 */
@DisplayName("ING-UNIT-010: raw_payload hash validation")
class PayloadHashValidatorTest {

    private static String b64(byte[] packet) {
        return Base64.getEncoder().encodeToString(packet);
    }

    private static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("valid packet bytes pass")
    void validPayloadPasses() {
        byte[] packet = {1, 2, 3, 4, 5, 40, 0, 2, 0, 0};
        assertEquals(PayloadHashValidator.Result.VALID,
                PayloadHashValidator.validate(b64(packet), sha256Hex(packet)));
        assertArrayEquals(packet, PayloadHashValidator.decodeValid(
                b64(packet), sha256Hex(packet)));
    }

    @Test
    @DisplayName("hash mismatch is rejected")
    void hashMismatchRejected() {
        byte[] packet = {1, 2, 3, 4, 5};
        assertEquals(PayloadHashValidator.Result.HASH_MISMATCH,
                PayloadHashValidator.validate(b64(packet), sha256Hex(new byte[]{9, 9, 9})));
        assertNull(PayloadHashValidator.decodeValid(
                b64(packet), sha256Hex(new byte[]{9, 9, 9})));
    }

    @Test
    @DisplayName("missing payload_hash is rejected")
    void missingHashRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_HASH,
                PayloadHashValidator.validate(b64(new byte[]{1, 2, 3}), null));
    }

    @Test
    @DisplayName("non-hex payload_hash is rejected")
    void nonHexHashRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_HASH,
                PayloadHashValidator.validate(b64(new byte[]{1, 2, 3}), "not-a-hash"));
    }

    @Test
    @DisplayName("invalid Base64 is rejected")
    void invalidBase64Rejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate("!!!not-base64!!!", sha256Hex(new byte[]{1})));
    }

    @Test
    @DisplayName("empty payload is rejected")
    void emptyPayloadRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(b64(new byte[0]), sha256Hex(new byte[0])));
    }

    @Test
    @DisplayName("missing raw_payload is rejected")
    void missingPayloadRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(null, sha256Hex(new byte[]{1})));
    }
}
