package com.trading.common.schema.execution;

import java.util.List;

/**
 * Durable boundary for the Execution_Gate table (v3, CHG-044). A gateway-backed
 * implementation persists rows to Fluss; the offline engine composes against
 * this interface and an in-memory implementation so the protocol (order of
 * persists, fence concurrency, approvals, halting) is proven before the Fluss
 * writer is wired. The store owns fence acquisition (concurrent-owner
 * rejection), two-person approval registration, and the epoch-incrementing
 * halt — the writes that must never be inferred from a raw KV upsert.
 */
public interface GateStateStore {

    /** Outcome of an approval registration. */
    enum ApprovalOutcome {
        /** A (first or second) distinct authorized approval was recorded. */
        APPLIED,
        /** Approvals are already complete for this epoch/evidence. */
        ALREADY_APPLIED,
        /** The same principal tried to approve twice — the two-person rule requires distinct operators. */
        SAME_PRINCIPAL,
        /** The principal is not authorized to approve this scope. */
        UNAUTHORIZED,
        /** The approval is for a gate epoch that no longer matches the current row epoch. */
        EPOCH_MISMATCH,
        /** No gate row exists for the partition. */
        NOT_FOUND
    }

    record ApprovalResult(ApprovalOutcome outcome, GateRow row, String reason) {
        public static ApprovalResult applied(GateRow row) {
            return new ApprovalResult(ApprovalOutcome.APPLIED, row, null);
        }
        public static ApprovalResult of(ApprovalOutcome o, GateRow row, String reason) {
            return new ApprovalResult(o, row, reason);
        }
    }

    /** Fence acquisition result. */
    record FenceResult(GateRow row, String owner, long token, boolean conflict) {
        public static FenceResult acquired(GateRow row, String owner, long token) {
            return new FenceResult(row, owner, token, false);
        }
        public static FenceResult conflict(GateRow row, String reason) {
            return new FenceResult(row, null, 0L, true);
        }
    }

    /** Immutable audit/evidence event appended on every meaningful write (Execution_Audit shape). */
    record AuditRecord(
            String partitionId,
            String eventType,
            long ts,
            long gateEpoch,
            long fenceToken,
            String detail,
            String evidenceHash) {}

    /** Current durable snapshot, or {@code null} if no row exists for the partition. */
    GateRow read(String partitionId);

    /** Create (or recreate) a gate row — boot state HALTED, epoch 0, no fence/approvals. */
    GateRow init(GateRow haltedBootRow);

    /**
     * Acquire (or refresh) the partition lease for {@code owner}. Fails closed
     * on a live lease held by a different owner (concurrent-owner rejection),
     * mirroring the monotonic fencing sequence: the returned token is always
     * strictly greater than any prior fence token for the partition.
     */
    FenceResult acquire(String partitionId, String ownerInstanceId, long leaseMs, long nowTs);

    /**
     * Register one of the two required approvals for the exact
     * {@code (epoch, evidenceHash)}. Distinct, authorized principals only; a
     * changed epoch invalidates pending approvals (the caller never approves a
     * different generation than the two-person gate authorized).
     */
    ApprovalResult approve(String partitionId, String principal, long epoch, String evidenceHash,
                           long nowTs);

    /**
     * Raise a safety halt: move the gate to HALTED and increment the epoch by
     * exactly one, from {@code expected}. Idempotent (already HALTED) halts
     * record evidence without a second epoch increment.
     */
    GateRow halt(String partitionId, GateRow expected, String reason, String evidenceHash, long nowTs);

    /** Append an immutable audit/evidence event. */
    void audit(AuditRecord record);

    /** The immutable audit log, in append order (crash-window reconstruction). */
    List<AuditRecord> auditLog();
}
