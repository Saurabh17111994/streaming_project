package com.trading.common.schema.projection;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Versioned content fingerprint for a {@link NormalizedPostback}
 * ({@code Fills.postback_fingerprint} / {@code fingerprint_version}).
 *
 * <p>The fingerprint is computed over the canonical identity + quantity +
 * price + side + timestamps so that an equal-version postback with a different
 * fingerprint is a content conflict, and a changed hash under the same
 * instruction is quarantined as {@link QuarantineReason#FINGERPRINT_MISMATCH}.
 */
public final class PostbackFingerprint {

    private PostbackFingerprint() {}

    /** Deterministic SHA-256 fingerprint over the canonical payload parts. */
    public static String compute(String mappingVersion, String... canonicalParts) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        StringBuilder sb = new StringBuilder();
        for (String part : canonicalParts) {
            sb.append(part == null ? "" : part).append('\u0000');
        }
        byte[] digest = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16))
               .append(Character.forDigit(b & 0xF, 16));
        }
        return "fp-" + mappingVersion + "-" + hex;
    }

    /**
     * Canonical parts for an order/fill postback — identity, side, cumulative,
     * pending, fill quantity, fill price, and event/receive times. Deterministic
     * and independent of wall-clock.
     */
    public static String[] canonicalParts(NormalizedPostback p) {
        return canonicalFrom(p.mappingVersion(), p.postbackEventId(), p.sourceEventId(),
                p.sourceSequence(), p.brokerOrderId(), p.echoedClientOrderRef(),
                p.accountScopeId(), p.side(), p.orderStatus(), p.cumulativeQty(),
                p.pendingQty(), p.fillQty(), p.fillPricePaise(), p.eventTimeMs(),
                p.receiveTimeMs());
    }

    /** Field-level canonical part builder (used by tests to mint valid fixtures). */
    public static String[] canonicalFrom(String mappingVersion, String postbackEventId,
            String sourceEventId, long sourceSequence, String brokerOrderId,
            String echoedClientOrderRef, String accountScopeId, String side,
            String orderStatus, long cumulativeQty, long pendingQty, long fillQty,
            long fillPricePaise, long eventTimeMs, long receiveTimeMs) {
        return new String[] {
            postbackEventId, sourceEventId, Long.toString(sourceSequence),
            brokerOrderId, echoedClientOrderRef, accountScopeId,
            side, orderStatus, Long.toString(cumulativeQty),
            Long.toString(pendingQty), Long.toString(fillQty),
            Long.toString(fillPricePaise), Long.toString(eventTimeMs),
            Long.toString(receiveTimeMs), mappingVersion
        };
    }

    /** True when the envelope's fingerprint matches a recompute over its content. */
    public static boolean matches(NormalizedPostback p) {
        String expected = compute(p.mappingVersion(), canonicalParts(p));
        return expected.equals(p.fingerprint());
    }
}
