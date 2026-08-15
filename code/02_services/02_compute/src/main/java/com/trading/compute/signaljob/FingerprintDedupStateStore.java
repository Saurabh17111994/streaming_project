package com.trading.compute.signaljob;

import java.util.List;

/**
 * Authoritative dedup state behind the bounded hot cache (DEC-038;
 * docs/08_implementation/04-signal-job.md §Design — fingerprint_dedup dedup
 * state table). Fluss owns the complete durable dedup set; the Flink working
 * state is only the bounded cache and is never a second copy.
 *
 * <p>The read path is authoritative: a fingerprint is a duplicate iff the
 * key exists AND {@code expiry_ms} is in the future. The cache consults this
 * store on miss (query-on-miss — lazy rehydration), so a cold cache after a
 * restart is correct, never an empty dedup set (SIG-STATE-003).
 *
 * <p>Raw-client notes (COMPAT-FLUSS-005): {@code fingerprint_dedup} has a
 * composite PK with a single-field bucket key subset and
 * {@code kv.format-version=2} — the documented-working combo for raw-client
 * upsert. Delete semantics on composite keys are evidence-gated
 * (SIG-STATE-001); the write path here is the same upsert machinery the
 * candle KV table already proves.
 */
public interface FingerprintDedupStateStore extends AutoCloseable {

    /** Read-path verdict for one fingerprint. */
    enum Lookup {
        /** Row exists and {@code expiry_ms} is in the future — duplicate. */
        SEEN_LIVE,
        /** Row exists but is logically expired — re-acceptable, stale row. */
        SEEN_EXPIRED,
        /** No row — first occurrence, acceptable. */
        NOT_SEEN
    }

    /**
     * Authoritative lookup for {@code (token, version, fingerprint)} at
     * {@code nowMs}. Never a false {@code SEEN_LIVE} after expiry — stale rows
     * are {@code SEEN_EXPIRED}.
     */
    Lookup lookup(long token, String version, String fingerprint, long nowMs) throws Exception;

    /**
     * Durable first-seen write: store {@code expiry_ms = first_seen_ms + TTL}
     * exactly ({@link DedupExpiry#expiryMs}). Idempotent — re-writing the same
     * key converges (KV last-write-wins).
     */
    void putFirstSeen(long token, String version, String fingerprint,
            long firstSeenMs, long expiryMs) throws Exception;

    /**
     * Bounded expired-row selection for one bucket (the token's bucket): the
     * earliest-expiring expired rows, capped at {@code maxBatchSize}, re-entrant
     * ({@link DedupExpiry#selectBatch}).
     */
    List<DedupExpiry.CleanupCandidate> scanExpired(long token, long nowMs,
            int maxBatchSize) throws Exception;

    /**
     * Durable delete of the selected expired rows. Idempotent; an interrupted
     * pass resumes on the next tick (re-entrant selection).
     */
    void delete(List<DedupExpiry.CleanupCandidate> batch) throws Exception;
}
