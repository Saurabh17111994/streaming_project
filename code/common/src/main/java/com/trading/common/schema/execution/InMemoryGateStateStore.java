package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link GateStateStore} — the offline durable boundary the
 * {ExecutionCommandGate} protocol engine and its crash-window/zero-duplicate
 * tests run against (T5, CHG-044). It is deliberately NOT a production cache:
 * it can only be a lease/fence authority when the real writer is a gateway-backed
 * Fluss store using the deployment leadership/fencing mechanism (ASM-EXE-005 /
 * REQ-EXE-012). A Fluss-backed implementation satisfies the same interface.
 *
 * <p>It enforces the writes that a raw KV upsert must never be trusted to
 * provide: monotonic fence tokens with concurrent-owner rejection, distinct
 * authorized two-person approvals tied to an exact (epoch, evidence hash), and
 * an exactly-once epoch increment on a safety halt.
 */
public final class InMemoryGateStateStore implements GateStateStore {

    private final Map<String, GateRow> rows = new LinkedHashMap<>();
    private final List<AuditRecord> audit = new ArrayList<>();
    private final AtomicLong fenceSequence = new AtomicLong();
    /** Authorized approvers; empty set means "any principal" (test seam). */
    private final Set<String> authorizedApprovers;

    public InMemoryGateStateStore() {
        this(Set.of());
    }

    public InMemoryGateStateStore(Set<String> authorizedApprovers) {
        this.authorizedApprovers = authorizedApprovers;
    }

    @Override
    public GateRow read(String partitionId) {
        return rows.get(partitionId);
    }

    @Override
    public synchronized GateRow init(GateRow boot) {
        GateRow existing = rows.get(boot.partitionId());
        if (existing == null) {
            rows.put(boot.partitionId(), boot);
            return boot;
        }
        return existing; // already initialized — never clobber a fenced gate
    }

    @Override
    public synchronized FenceResult acquire(String partitionId, String owner, long leaseMs,
                                            long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return FenceResult.conflict(null, "no gate row for partition " + partitionId);
        }
        // Concurrent-owner rejection: a live (unexpired) lease held by a
        // DIFFERENT instance blocks acquisition. Same owner refreshes.
        if (row.ownerInstanceId() != null && !row.fenceExpiredAt(nowTs)
                && !row.ownerInstanceId().equals(owner)) {
            return FenceResult.conflict(row,
                    "live lease held by " + row.ownerInstanceId());
        }
        long token = fenceSequence.incrementAndGet();
        GateRow next = row.withFence(owner, token, nowTs, leaseMs);
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "FENCE_ACQUIRE", nowTs, next.epoch(), token,
                "owner=" + owner, null));
        return FenceResult.acquired(next, owner, token);
    }

    @Override
    public synchronized ApprovalResult approve(String partitionId, String principal,
                                               long epoch, String evidenceHash, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return ApprovalResult.of(ApprovalOutcome.NOT_FOUND, null, "no gate row");
        }
        if (row.epoch() != epoch) {
            return ApprovalResult.of(ApprovalOutcome.EPOCH_MISMATCH, row,
                    "approval epoch " + epoch + " != current gate epoch " + row.epoch());
        }
        if (!authorizedApprovers.isEmpty() && !authorizedApprovers.contains(principal)) {
            return ApprovalResult.of(ApprovalOutcome.UNAUTHORIZED, row,
                    "principal " + principal + " not authorized");
        }
        // Distinct principals only. Second approval must reference the same evidence hash.
        if (row.approval1() != null && row.approval1().equals(principal)) {
            return ApprovalResult.of(ApprovalOutcome.SAME_PRINCIPAL, row,
                    "approver " + principal + " already approved once");
        }
        if (row.approval2() != null && row.approval2().equals(principal)) {
            return ApprovalResult.of(ApprovalOutcome.SAME_PRINCIPAL, row,
                    "approver " + principal + " already approved once");
        }
        if (row.approvalsComplete()) {
            return ApprovalResult.of(ApprovalOutcome.ALREADY_APPLIED, row, "approvals complete");
        }
        GateRow next;
        if (row.approval1() == null) {
            next = new GateRow(row.partitionId(), row.accountScopeId(), row.state(), row.epoch(),
                    row.reason(), row.evidenceHash(), principal, null, evidenceHash,
                    row.ownerInstanceId(), row.fenceToken(), row.fenceAcquiredTs(),
                    row.leaseExpiresTs(), row.fenceLostTs());
        } else {
            if (!evidenceHash.equals(row.approvedEvidenceHash())) {
                return ApprovalResult.of(ApprovalOutcome.EPOCH_MISMATCH, row,
                        "second approval evidence hash differs from first — not the same package");
            }
            next = new GateRow(row.partitionId(), row.accountScopeId(), row.state(), row.epoch(),
                    row.reason(), row.evidenceHash(), row.approval1(), principal,
                    evidenceHash, row.ownerInstanceId(), row.fenceToken(), row.fenceAcquiredTs(),
                    row.leaseExpiresTs(), row.fenceLostTs());
        }
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "APPROVE", nowTs, next.epoch(), next.fenceToken(),
                "principal=" + principal, evidenceHash));
        return ApprovalResult.applied(next);
    }

    @Override
    public synchronized GateRow halt(String partitionId, GateRow expected, String reason,
                                     String evidenceHash, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return null;
        }
        GateRow next;
        if (row.state() == GateState.HALTED) {
            // Idempotent safe halt while already HALTED: record evidence, no second epoch increment.
            next = row;
        } else {
            next = row.withState(GateState.HALTED, reason, evidenceHash == null
                    ? row.evidenceHash() : evidenceHash, nowTs);
        }
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "HALT", nowTs, next.epoch(), next.fenceToken(),
                reason, evidenceHash));
        return next;
    }

    @Override
    public synchronized void audit(AuditRecord record) {
        audit.add(record);
    }

    @Override
    public synchronized List<AuditRecord> auditLog() {
        return List.copyOf(audit);
    }

    /** Test/migration helper: directly install a gate row (e.g. an already-ENABLED fenced gate). */
    public synchronized void install(GateRow row) {
        rows.put(row.partitionId(), row);
    }

    /**
     * Restart-refresh: install a durable gate row recovered from Fluss and advance the local
     * fence sequence to at least its token, so a subsequent {@link #acquire} in this process is
     * still strictly greater than any token already issued durably (monotonic across a restart).
     */
    public synchronized void hydrate(GateRow row) {
        Objects.requireNonNull(row, "row");
        fenceSequence.accumulateAndGet(row.fenceToken(), Math::max);
        rows.put(row.partitionId(), row);
    }
}
