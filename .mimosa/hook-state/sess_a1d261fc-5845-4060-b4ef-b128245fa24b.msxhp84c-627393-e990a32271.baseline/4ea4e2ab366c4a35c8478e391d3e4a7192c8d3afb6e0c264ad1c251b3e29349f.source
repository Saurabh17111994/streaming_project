package com.trading.common.safety;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * SHA-256 digest of a token set, byte-identical to the ingestion contract:
 * tokens sorted ascending, each encoded as 8 big-endian bytes, digest
 * hex-encoded lowercase.
 *
 * <p>Parity targets (verified by pinned vectors in the test suite):
 * <ul>
 *   <li>Go {@code tokenSetHash} (go-bridge/subscription_plan.go) and</li>
 *   <li>Java {@code SafetyHaltWriter.computeAssignedTokenHash} /
 *       {@code InstrumentManifestLoader.computeFingerprint}.</li>
 * </ul>
 * Pinned vector: {@code [1000, 1001, 1]} &rarr;
 * {@code 8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c}
 * (order-independent by construction).
 */
public final class TokenSetHash {

    private TokenSetHash() {}

    /** Lowercase SHA-256 hex over the sorted 8-byte-big-endian encoding. */
    public static String of(Collection<Long> tokens) {
        List<Long> ordered = new ArrayList<>(tokens);
        ordered.sort(Comparator.naturalOrder());
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        for (long token : ordered) {
            buf.clear();
            sha256.update(buf.putLong(token).array());
        }
        return toHex(sha256.digest());
    }

    /** Varargs convenience overload. */
    public static String of(long... tokens) {
        List<Long> list = new ArrayList<>(tokens.length);
        for (long t : tokens) {
            list.add(t);
        }
        return of(list);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }
        return sb.toString();
    }
}
