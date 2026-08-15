package com.trading.compute.signaljob;

import java.util.Comparator;
import java.util.List;

/**
 * Pure expiry + bounded cleanup-selection logic for the {@code fingerprint_dedup}
 * state table (DEC-038; design: docs/08_implementation/04-signal-job.md
 * §Design — fingerprint_dedup dedup state table).
 *
 * <p>Fluss 0.9.1 has no per-key TTL, so logical expiry is writer-enforced:
 *
 * <ol>
 *   <li>the write path stores {@code expiry_ms = first_seen_ms + DEDUP_TTL_MS}
 *       exactly — {@link #expiryMs};</li>
 *   <li>the read path treats a row as a duplicate iff the key exists AND
 *       {@code expiry_ms} is in the future — {@link #isExpired} makes stale
 *       rows harmless, never a false "seen" after expiry;</li>
 *   <li>the cleanup pass deletes rows with {@code expiry_ms < now} in bounded
 *       batches on {@code DEDUP_CLEANUP_INTERVAL_MS} — {@link #selectBatch}
 *       picks the earliest-expiring keys first (deterministic, re-entrant, so
 *       an interrupted pass resumes on the next tick and never loses rows).</li>
 * </ol>
 *
 * <p>The exact Fluss delete/ack semantics of the live cleanup pass are
 * evidence-gated (SIG-STATE-001/002); this class is the pure, unit-tested
 * selection/expiry core the live pass drives. Growth is bounded by
 * construction: entries = accepted rate × TTL horizon.
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
