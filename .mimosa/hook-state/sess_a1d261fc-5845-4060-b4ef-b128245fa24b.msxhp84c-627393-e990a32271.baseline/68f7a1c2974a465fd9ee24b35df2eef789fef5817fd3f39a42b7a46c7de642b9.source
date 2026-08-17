package com.trading.compute.signaljob;

import java.util.Comparator;
import java.util.List;

/**
 * Pure expiry logic for fingerprint deduplication (design B, 2026-08-16:
 * the dedup set is authoritative Flink keyed state).
 *
 * <p>{@link #expiryMs} defines the exact event-time expiry used by
 * {@link FingerprintDedupFunction}: {@code first_seen_ms + DEDUP_TTL_MS}.
 * {@link #isExpired} and {@link #selectBatch} remain as the reference
 * semantics of the retired Fluss-backed dedup store (DEC-038) — they are
 * unit-tested here and kept for test-only use; the live operator expires via
 * event-time timers, not these helpers.
 */
public final class DedupExpiry {

    private DedupExpiry() {}

    /** A dedup row selected for deletion. The key is the composite row identity. */
    public record CleanupCandidate(String key, long firstSeenMs, long expiryMs) {}

    /** Exactly {@code firstSeenMs + ttlMs} — entries are never expired early. */
    public static long expiryMs(long firstSeenMs, long ttlMs) {
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("dedup TTL must be positive, got " + ttlMs);
        }
        return firstSeenMs + ttlMs;
    }

    /** Logically expired iff {@code expiryMs < now} (strict — equality is still live). */
    public static boolean isExpired(long expiryMs, long nowMs) {
        return expiryMs < nowMs;
    }

    /**
     * Select the bounded cleanup batch: the expired candidates with the
     * earliest expiry (ties broken by key for determinism), capped at
     * {@code maxBatchSize}. Re-entrant — the next pass continues with the
     * remaining expired rows.
     *
     * @throws IllegalArgumentException when {@code maxBatchSize <= 0} or a
     *         candidate's expiry is before its first-seen (corrupt row)
     */
    public static List<CleanupCandidate> selectBatch(List<CleanupCandidate> candidates,
            long nowMs, int maxBatchSize) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("cleanup batch size must be positive, got "
                    + maxBatchSize);
        }
        if (candidates == null) {
            return List.of();
        }
        // Fail closed on corrupt rows: an expiry before first-seen means the
        // row was written by a buggy/forked writer — deleting it would silently
        // widen the dedup window, keeping it would wedge it. Surface the row.
        for (CleanupCandidate c : candidates) {
            if (c.expiryMs() < c.firstSeenMs()) {
                throw new IllegalArgumentException("corrupt dedup row " + c.key()
                        + ": expiry_ms " + c.expiryMs() + " before first_seen_ms "
                        + c.firstSeenMs());
            }
        }
        return candidates.stream()
                .filter(c -> isExpired(c.expiryMs(), nowMs))
                .sorted(Comparator.comparingLong(CleanupCandidate::expiryMs)
                        .thenComparing(CleanupCandidate::key))
                .limit(maxBatchSize)
                .toList();
    }
}
