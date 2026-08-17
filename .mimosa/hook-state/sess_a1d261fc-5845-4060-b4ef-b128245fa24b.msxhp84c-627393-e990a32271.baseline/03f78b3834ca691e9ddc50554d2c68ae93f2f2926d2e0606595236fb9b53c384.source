package com.trading.ingestion.fingerprint;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Versioned canonical event-fingerprint builder.
 *
 * <p>Produces a deterministic content hash that identifies duplicates within
 * a connection epoch. The contract (DEC-012, DEC-015):
 *
 * <ul>
 *   <li>Fingerprint is best-effort, not broker-global identity</li>
 *   <li>Collision risk and identical-legitimate-event limitation are acknowledged</li>
 *   <li>Hash collisions do not create a safety vulnerability — Compute owns
 *       logical deduplication</li>
 * </ul>
 *
 * <p><b>Fingerprint version 1 (SHA-256, canonical field order):</b>
 * <pre>{@code
 *   connection_epoch || instrument_token || event_time_ms || tick_type
 *   || last_price_paise || last_qty || bid_price_paise || ask_price_paise
 * }</pre>
 *
 * <p>All integers are big-endian 8-byte. Strings are UTF-8. Null/missing
 * values are encoded as zero or empty string. Delimiter is {@code |}.
 *
 * <p>Thread-safe: stateless, no shared buffers.
 */
public final class FingerprintBuilder {

    /** Current fingerprint algorithm version. Bump when the canonical form changes. */
    public static final int FINGERPRINT_VERSION = 1;
    private static final String ALGORITHM = "SHA-256";
    private static final byte DELIM = '|';

    private FingerprintBuilder() {}

    /**
     * Build a version-1 canonical fingerprint.
     *
     * @param connectionEpoch  monotonically-increasing connection epoch
     * @param instrumentToken  Arrow instrument token
     * @param eventTimeEpochMs UTC epoch ms
     * @param tickType         "TRADE" or "QUOTE"
     * @param lastPricePaise   last traded price in paise (0 if not a trade)
     * @param lastQty          last traded quantity (0 if not a trade)
     * @param bidPricePaise    best bid in paise (0 if not available)
     * @param askPricePaise    best ask in paise (0 if not available)
     * @return lowercase hex-encoded SHA-256 digest
     */
    public static Result build(long connectionEpoch,
                               long instrumentToken,
                               long eventTimeEpochMs,
                               String tickType,
                               long lastPricePaise,
                               long lastQty,
                               long bidPricePaise,
                               long askPricePaise) {
        MessageDigest md = sha256();
        // canonical field order, big-endian, pipe-delimited
        writeLong(md, connectionEpoch);
        md.update(DELIM);
        writeLong(md, instrumentToken);
        md.update(DELIM);
        writeLong(md, eventTimeEpochMs);
        md.update(DELIM);
        writeStr(md, tickType);
        md.update(DELIM);
        writeLong(md, lastPricePaise);
        md.update(DELIM);
        writeLong(md, lastQty);
        md.update(DELIM);
        writeLong(md, bidPricePaise);
        md.update(DELIM);
        writeLong(md, askPricePaise);

        byte[] digest = md.digest();
        return new Result(HexFormat.of().formatHex(digest), FINGERPRINT_VERSION, ALGORITHM);
    }

    /** Fingerprint result carrying hash, version, and algorithm metadata. */
    public record Result(String hash, int version, String algorithm) {
        @Override
        public String toString() {
            return hash.substring(0, Math.min(12, hash.length())) + " (v" + version + ")";
        }
    }

    // ---- internal helpers ----

    private static void writeLong(MessageDigest md, long v) {
        md.update((byte) (v >>> 56));
        md.update((byte) (v >>> 48));
        md.update((byte) (v >>> 40));
        md.update((byte) (v >>> 32));
        md.update((byte) (v >>> 24));
        md.update((byte) (v >>> 16));
        md.update((byte) (v >>> 8));
        md.update((byte) (v));
    }

    private static void writeStr(MessageDigest md, String s) {
        if (s == null) return;
        md.update(s.getBytes(StandardCharsets.UTF_8));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance(ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
