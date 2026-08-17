package com.trading.common.schema.eod;

import java.util.List;

/**
 * Durable store for EOD offload state (SCH-23): the controller's state must
 * be durable with restart/resume (docs/04_contracts/02-storage.md "EOD
 * controller"), and every transition goes through the record's validated
 * state machine — so a crashed controller re-runs a day idempotently instead
 * of losing or regressing it.
 *
 * <p>Two implementations: {@link FlussEodStateStore} (the raw client against
 * the {@code eod_offload_state} KV table — the authoritative home) and the
 * in-memory store used by the unit tests. The interface is deliberately tiny:
 * read-all (current state, folded by record id), upsert (last-write-wins —
 * the KV convergence semantic that makes re-runs idempotent), and the
 * single-writer lease.
 */
public interface EodStateStore {

    /** All offload records currently on file (the lease row is excluded). */
    List<EodOffloadRecord> readAll() throws Exception;

    /** Upsert one offload record (last-write-wins — replay/re-run converges). */
    void upsert(EodOffloadRecord record) throws Exception;

    /**
     * Single-writer fencing (best-effort — the raw client has no atomic
     * compare-and-set, so the lease is read-then-write with a token + expiry;
     * a crashed holder blocks until its lease expires). Returns the lease
     * NOW in effect: either freshly acquired (this {@code token}, expiry
     * {@code now + ttl}) or the unexpired lease of another holder — the
     * caller refuses to run when the returned token is not its own.
     */
    Lease acquireLease(String token, long nowMs, long leaseTtlMs) throws Exception;
}
