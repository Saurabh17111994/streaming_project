package com.trading.common.schema.eod;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link EodStateStore} for the EOD controller unit tests: same
 * semantics as the Fluss-backed store (upsert by record id, last-write-wins,
 * lease read-then-write), no cluster.
 */
public final class InMemoryEodStateStore implements EodStateStore {

    private final Map<String, EodOffloadRecord> records = new LinkedHashMap<>();
    private Lease lease;

    @Override
    public List<EodOffloadRecord> readAll() {
        return new ArrayList<>(records.values());
    }

    @Override
    public void upsert(EodOffloadRecord record) {
        records.put(EodOffloadStateColumns.recordId(record.tradingDate(), record.tableName()),
                record);
    }

    @Override
    public Lease acquireLease(String token, long nowMs, long leaseTtlMs) {
        if (lease != null && lease.expiryMs() >= nowMs && !token.equals(lease.token())) {
            return lease; // held by another, unexpired — refuse
        }
        lease = new Lease(token, nowMs + leaseTtlMs, nowMs);
        return lease;
    }

    public Map<String, EodOffloadRecord> raw() {
        return records;
    }
}
