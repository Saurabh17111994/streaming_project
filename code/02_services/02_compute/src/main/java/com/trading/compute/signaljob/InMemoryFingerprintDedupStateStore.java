package com.trading.compute.signaljob;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link FingerprintDedupStateStore} — the default store factory for
 * unit/harness runs and the reference semantics for the raw-client twin.
 * Rows are {@code token|version|fingerprint -> {firstSeenMs, expiryMs}}.
 */
public final class InMemoryFingerprintDedupStateStore implements FingerprintDedupStateStore {

    private final Map<String, long[]> rows = new ConcurrentHashMap<>();
    private final List<String> deletedKeys = new ArrayList<>();

    private static String key(long token, String version, String fingerprint) {
        return token + "|" + version + "|" + fingerprint;
    }

    @Override
    public Lookup lookup(long token, String version, String fingerprint, long nowMs) {
        long[] v = rows.get(key(token, version, fingerprint));
        if (v == null) {
            return Lookup.NOT_SEEN;
        }
        return DedupExpiry.isExpired(v[1], nowMs) ? Lookup.SEEN_EXPIRED : Lookup.SEEN_LIVE;
    }

    @Override
    public void putFirstSeen(long token, String version, String fingerprint,
            long firstSeenMs, long expiryMs) {
        rows.put(key(token, version, fingerprint), new long[] {firstSeenMs, expiryMs});
    }

    @Override
    public List<DedupExpiry.CleanupCandidate> scanExpired(long token, long nowMs,
            int maxBatchSize) {
        List<DedupExpiry.CleanupCandidate> candidates = new ArrayList<>();
        for (Map.Entry<String, long[]> e : rows.entrySet()) {
            String[] parts = e.getKey().split("\\|", 3);
            candidates.add(new DedupExpiry.CleanupCandidate(
                    e.getKey(), e.getValue()[0], e.getValue()[1]));
        }
        return DedupExpiry.selectBatch(candidates, nowMs, maxBatchSize);
    }

    @Override
    public void delete(List<DedupExpiry.CleanupCandidate> batch) {
        for (DedupExpiry.CleanupCandidate c : batch) {
            if (rows.remove(c.key()) != null) {
                deletedKeys.add(c.key());
            }
        }
    }

    @Override
    public void close() {
        // nothing to release
    }

    /** Deleted keys since open (test observation). */
    public List<String> deletedKeys() {
        return List.copyOf(deletedKeys);
    }

    /** Current row count (test observation). */
    public int rowCount() {
        return rows.size();
    }
}
