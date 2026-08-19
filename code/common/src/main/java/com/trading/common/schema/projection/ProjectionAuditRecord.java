package com.trading.common.schema.projection;

/** Row mirror of Execution_Audit (14_execution_audit.sql) for T6 projections. */
public record ProjectionAuditRecord(
        String auditEventId,
        String eventType,
        String instructionId,
        String executionAttemptId,
        String executionPartitionId,
        String accountScopeId,
        long gateEpoch,
        String actorId,
        String evidenceHash,
        String evidenceSummary,
        long eventTs,
        String schemaVersion) {
}
