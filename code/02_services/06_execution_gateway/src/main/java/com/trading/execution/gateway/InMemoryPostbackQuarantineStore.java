package com.trading.execution.gateway;

import java.util.ArrayList;
import java.util.List;

/**
 * Tier 0 #6 in-memory stub for {@link PostbackQuarantineStore}.
 * Offline-test twin of the Fluss append path; keeps rows in JVM memory.
 */
public final class InMemoryPostbackQuarantineStore implements PostbackQuarantineStore {

    public record QuarantineRecord(
            String postbackEventId,
            String reason,
            String evidenceSummary,
            byte[] rawPayload) {}

    private final List<QuarantineRecord> rows = new ArrayList<>();

    @Override
    public void quarantine(String postbackEventId, String reason, String evidenceSummary, byte[] rawPayload) {
        rows.add(new QuarantineRecord(postbackEventId, reason, evidenceSummary,
                rawPayload == null ? new byte[0] : rawPayload.clone()));
    }

    public List<QuarantineRecord> all() {
        return List.copyOf(rows);
    }

    public int size() {
        return rows.size();
    }

    public void clear() {
        rows.clear();
    }
}
