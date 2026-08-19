package com.trading.common.schema.projection;

import java.util.Optional;

/** Durable Postback_Projection_Ledger KV store keyed by {@code postback_event_id}. */
public interface ProjectionLedgerStore {

    Optional<ProjectionLedgerEntry> lookup(String postbackEventId) throws Exception;

    void put(ProjectionLedgerEntry entry) throws Exception;
}
