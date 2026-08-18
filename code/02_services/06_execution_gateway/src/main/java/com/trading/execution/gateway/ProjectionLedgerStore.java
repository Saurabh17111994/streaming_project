package com.trading.execution.gateway;

import java.util.List;

/** Durable store for the cross-table projection workflow. */
public interface ProjectionLedgerStore extends AutoCloseable {
    record Entry(String eventId, ProjectionLedger.State state, String expectedPriorState,
                 int retryCount, String lastError, String disposition, long stepTs, Long completedTs) {}
    Entry lookup(String eventId) throws Exception;
    void put(Entry entry) throws Exception;
    List<Entry> incomplete() throws Exception;
    @Override void close() throws Exception;
}
