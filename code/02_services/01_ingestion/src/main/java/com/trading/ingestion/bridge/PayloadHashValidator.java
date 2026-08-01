package com.trading.ingestion.bridge;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;

/**
 * Validates a tick's original-bytes hash (plan §Tick / §Data Flow).
 *
 * <p>The Go bridge emits {@code raw_payload} = Base64 of the exact decompressed
 * broker packet bytes and {@code payload_hash} = SHA-256 hex of those bytes.
 * Decoded JSON must not replace those bytes. This helper decodes and verifies;
 * the caller decides quarantine/append side effects.
 *
 * <p>Pure logic — no Fluss or OTLP dependency, unit-testable in isolation.
 */
public final class PayloadHashValidator {

    /** Outcome of validation. */
    public enum Result {
        /** Packet bytes are valid — caller may append. */
        VALID,
        /** raw_payload missing, not valid Base64, or empty. */
        MALFORMED_PAYLOAD,
        /** payload_hash missing or not a lowercase SHA-256 hex digest. */
        MALFORMED_HASH,
        /** SHA-256 of decoded bytes does not match payload_hash. */
        HASH_MISMATCH
    }

    private PayloadHashValidator() {}

    /**
     * Decode and validate. On {@link Result#VALID} returns the exact packet
     * bytes; otherwise returns {@code null} (caller handles quarantine).
     */
    public static byte[] validate(String rawPayloadB64, String payloadHash, Result[] out) {
        if (payloadHash == null || !payloadHash.matches("[0-9a-f]{64}")) {
            out[0] = Result.MALFORMED_HASH;
            return null;
        }
        if (rawPayloadB64 == null || rawPayloadB64.isBlank()) {
            out[0] = Result.MALFORMED_PAYLOAD;
            return null;
        }
        byte[] packet;
        try {
            packet = Base64.getDecoder().decode(rawPayloadB64);
        } catch (IllegalArgumentException e) {
            out[0] = Result.MALFORMED_PAYLOAD;
            return null;
        }
        if (packet.length == 0) {
            out[0] = Result.MALFORMED_PAYLOAD;
            return null;
        }
        String actual = HexFormat.of().formatHex(sha256(packet));
        if (!actual.equals(payloadHash)) {
            out[0] = Result.HASH_MISMATCH;
            return null;
        }
        out[0] = Result.VALID;
        return packet;
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
