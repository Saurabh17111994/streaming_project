package com.trading.common.schema.eod;

/**
 * Single-writer lease for the EOD controller (SCH-23): the runner acquires
 * the lease before a run and refuses to proceed while another controller's
 * token holds it unexpired. Best-effort fencing — the Fluss raw client has no
 * atomic compare-and-set, so the lease is a read-then-write token + expiry;
 * a crashed holder only blocks until its lease expires.
 */
public record Lease(String token, long expiryMs, long acquiredAtMs) {

    /** Held by this controller iff the token matches and the lease is live. */
    public boolean isHeldBy(String otherToken, long nowMs) {
        return otherToken != null && otherToken.equals(token) && !isExpired(nowMs);
    }

    public boolean isExpired(long nowMs) {
        return expiryMs < nowMs;
    }

    @Override
    public String toString() {
        return "Lease{token=" + token + ", expiryMs=" + expiryMs
                + ", acquiredAtMs=" + acquiredAtMs + "}";
    }
}
