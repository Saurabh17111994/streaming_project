package com.trading.capture;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Deterministic fingerprint for a broker postback, mirroring
 * {@code go-bridge/postback.go#NormalizeOrderUpdate}:
 *
 * <pre>{@code
 * report.PostbackEventID = fingerprint(map[string]string{
 *   "id":                     brokerOrderID,
 *   "remarks":                clientOrderRef,
 *   "status":                 orderStatus,
 *   "report_type":            reportType,
 *   "fill_shares":            fillShares,
 *   "average_price":          averagePrice,
 *   "exchange_update_time":   exchangeUpdateTime,
 * })
 * }</pre>
 *
 * <p>Canonical form: each entry as {@code key=value}, keys sorted lexicographically,
 * joined by {@code |}, then SHA-256 hex (lowercase).
 *
 * <p>Pure logic — no Fluss.
 */
public final class PostbackFingerprint {

    private PostbackFingerprint() {}

    /**
     * Compute deterministic SHA-256 hex of the canonical string for the supplied fields.
     *
     * <p>Canonical: sorted keys, each as {@code key=value}, joined by {@code |}.
     * Null values are treated as empty strings. Null map is treated as empty
     * canonical string (sha256 of {@code ""}).
     *
     * @param fields map with keys {@code id, remarks, status, report_type,
     *               fill_shares, average_price, exchange_update_time}
     * @return lowercase hex-encoded SHA-256 digest (64 chars)
     */
    public static String fingerprint(Map<String, String> fields) {
        if (fields == null || fields.isEmpty()) {
            return sha256Hex("");
        }
        List<String> keys = new ArrayList<>(fields.keySet());
        Collections.sort(keys);
        StringBuilder canonical = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            String k = keys.get(i);
            String v = fields.get(k);
            if (v == null) {
                v = "";
            }
            canonical.append(k).append('=').append(v);
            if (i < keys.size() - 1) {
                canonical.append('|');
            }
        }
        return sha256Hex(canonical.toString());
    }

    /**
     * SHA-256 hex of the UTF-8 bytes of {@code input} (null → empty string).
     *
     * @param input string to hash
     * @return lowercase hex-encoded SHA-256 digest
     */
    public static String sha256Hex(String input) {
        if (input == null) {
            input = "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
