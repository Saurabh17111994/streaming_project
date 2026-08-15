package com.trading.common.schema.execution;

import com.trading.common.schema.ownership.ExecutionAttemptsColumns;

/**
 * Immutable Execution_Attempts row
 * ({@code code/01_platform/02_sql/ddl/12_execution_attempts.sql} v2), in DDL
 * column order — the executor attempt protocol's durable record
 * (docs/08_implementation/07-executor.md &rarr; "Attempt rules").
 *
 * <p>Identity columns are fixed at minting and never rewritten: an attempt's
 * {@code execution_attempt_id}, scope, {@code request_hash}, and
 * {@code client_order_ref} identify it for its whole life. The mutable group
 * ({@code phase}/{@code phase_epoch}/{@code retry_attempt}) is owned by the
 * attempt-store; the call-evidence group (broker order id, outcome,
 * submitted/terminal timestamps, response summary) is owned by the
 * broker-adapter (see {@code ExecutionAttemptsColumnOwnership}).
 */
public record AttemptRecord(
        String executionAttemptId,
        String accountScopeId,
        String instructionId,
        String actionId,
        String executionPartitionId,
        String requestHash,
        String clientOrderRef,
        String brokerOrderId,
        long gateEpoch,
        String phase,
        long phaseEpoch,
        String outcome,
        String outcomeDetail,
        long preparedTs,
        Long submittedTs,
        Long terminalTs,
        String brokerResponseSummary,
        int retryAttempt,
        String schemaVersion) {

    public static final String PHASE_PREPARED = "PREPARED";

    /**
     * Mints the PREPARED attempt (phase_epoch = 0, retry_attempt = 0,
     * prepared_ts = nowTs, call-evidence columns null). Caller supplies the
     * deterministic identities — including {@code execution_attempt_id} and
     * {@code client_order_ref} — per the dossier (no UUID / wall-clock inside
     * the store).
     */
    public static AttemptRecord prepared(String executionAttemptId, String accountScopeId,
                                         String instructionId, String actionId,
                                         String executionPartitionId, String requestHash,
                                         String clientOrderRef, long gateEpoch, long nowTs) {
        return new AttemptRecord(executionAttemptId, accountScopeId, instructionId, actionId,
                executionPartitionId, requestHash, clientOrderRef, null, gateEpoch,
                PHASE_PREPARED, 0L, null, null, nowTs, null, null, null, 0,
                ExecutionAttemptsColumns.SCHEMA_VERSION_V2);
    }
}
