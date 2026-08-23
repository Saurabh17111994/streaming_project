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
 * tests run against (T5, CHG-044; CHG-056 single-operator). It is deliberately NOT a production cache:
 * it can only be a lease/fence authority when the real writer is a gateway-backed
 * Fluss store using the deployment leadership/fencing mechanism (ASM-EXE-005 /
 * REQ-EXE-012). A Fluss-backed implementation satisfies the same interface.
 *
 * <p>It enforces the writes that a raw KV upsert must never be trusted to
 * provide: monotonic fence tokens with concurrent-owner rejection, single-operator
 * (Saurabh) approval tied to an exact (epoch, evidence hash), and
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
    public synchronized FenceResult renew(String partitionId, String ownerInstanceId, long fenceToken,
                                          long leaseMs, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return FenceResult.conflict(null, "no gate row for partition " + partitionId);
        }
        if (row.ownerInstanceId() == null) {
            return FenceResult.conflict(row, "no live lease to renew");
        }
        if (!row.ownerInstanceId().equals(ownerInstanceId)) {
            return FenceResult.conflict(row,
                    "renew rejected: holder is " + row.ownerInstanceId() + " not " + ownerInstanceId);
        }
        if (row.fenceToken() != fenceToken) {
            return FenceResult.conflict(row,
                    "renew rejected: token mismatch expected " + row.fenceToken() + " got " + fenceToken);
        }
        if (row.fenceExpiredAt(nowTs)) {
            return FenceResult.conflict(row,
                    "renew rejected: lease expired at " + row.leaseExpiresTs() + " now " + nowTs);
        }
        // Success: extend lease without changing fenceToken (offline lease semantics)
        GateRow next = row.withRenewedLease(nowTs, leaseMs);
        rows.put(partitionId, next);
        audit(new AuditRecord(partitionId, "FENCE_RENEW", nowTs, next.epoch(), next.fenceToken(),
                "owner=" + ownerInstanceId + " leaseMs=" + leaseMs, null));
        return FenceResult.acquired(next, ownerInstanceId, fenceToken);
    }

    @Override
    public synchronized GateRow revoke(String partitionId, String ownerInstanceId, long nowTs) {
        GateRow row = rows.get(partitionId);
        if (row == null) {
            return null;
        }
        if (row.ownerInstanceId() == null) {
            // already cleared — idempotent
            return row;
        }
        if (!row.ownerInstanceId().equals(ownerInstanceId)) {
            // not holder — fail closed, no mutation (offline fencing constraint)
            return row;
        }
        GateRow cleared = row.withFenceCleared(nowTs);
        rows.put(partitionId, cleared);
        audit(new AuditRecord(partitionId, "FENCE_REVOKE", nowTs, cleared.epoch(), cleared.fenceToken(),
                "owner=" + ownerInstanceId, null));
        return cleared;
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
        if (row.approvalsComplete()) {
            return ApprovalResult.of(ApprovalOutcome.ALREADY_APPLIED, row, "approvals complete");
        }
        if (row.approval1() != null && row.approval1().equals(principal)) {
            return ApprovalResult.of(ApprovalOutcome.SAME_PRINCIPAL, row,
                    "approver " + principal + " already approved once");
        }
        if (row.approval1() != null) {
            // Single-operator gate (DEC-044): one approval completes the gate.
            // A second distinct principal after completion is ALREADY_APPLIED;
            // we already returned above for complete, so this is a stale second
            // distinct approval on an incomplete row — should not happen, but
            // treat as already applied to avoid a second slot.
            return ApprovalResult.of(ApprovalOutcome.ALREADY_APPLIED, row, "approvals complete");
        }
        GateRow next = new GateRow(row.partitionId(), row.accountScopeId(), row.state(), row.epoch(),
                row.reason(), row.evidenceHash(), principal, null, evidenceHash,
                row.ownerInstanceId(), row.fenceToken(), row.fenceAcquiredTs(),
                row.leaseExpiresTs(), row.fenceLostTs());
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
            // HALTED default is fenced-off: ensure fence is cleared even on idempotent halt.
            next = row.withFenceCleared(nowTs);
            // if fence already cleared, withFenceCleared returns same instance — still audit
            if (next == row) {
                // no fence to clear, keep row as-is for idempotent epoch semantics
                next = row;
            }
        } else {
            GateRow halted = row.withState(GateState.HALTED, reason, evidenceHash == null
                    ? row.evidenceHash() : evidenceHash, nowTs);
            next = halted.withFenceCleared(nowTs);
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
