package com.trading.common.schema.projection;

/** Row mirror of Postback_Projection_Ledger (17_postback_projection_ledger.sql). */
public record ProjectionLedgerEntry(
        String postbackEventId,
        PostbackProjectionLedger.State projectionState,
        String expectedPriorState,
        int retryCount,
        String lastError,
        String disposition,
        long stepTs,
        Long completedTs,
        String schemaVersion) {
}
