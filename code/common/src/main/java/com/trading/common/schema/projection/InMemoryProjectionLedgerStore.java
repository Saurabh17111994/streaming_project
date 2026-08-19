package com.trading.common.schema.projection;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Pure-JVM {@link ProjectionLedgerStore} (offline tests/drills). */
public final class InMemoryProjectionLedgerStore implements ProjectionLedgerStore {

    private final Map<String, ProjectionLedgerEntry> rows = new HashMap<>();

    @Override
    public Optional<ProjectionLedgerEntry> lookup(String postbackEventId) {
        return Optional.ofNullable(rows.get(postbackEventId));
    }

    @Override
    public void put(ProjectionLedgerEntry entry) {
        rows.put(entry.postbackEventId(), entry);
    }

    public int size() {
        return rows.size();
    }
}
