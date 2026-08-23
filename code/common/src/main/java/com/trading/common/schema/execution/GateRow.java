package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import java.util.Objects;

/**
 * Durable snapshot of one Execution_Gate row (v3, CHG-044) — the authorization
 * surface a money-moving command is checked against. It carries the gate
 * lifecycle ({@code state}/{@code epoch}), the single-operator (DEC-044) approval
 * evidence ({@code approval_1}/{@code approval_2}/{@code approvedEvidenceHash} —
 * a second approval is not required and not checked), and the
 * fenced lease ({@code ownerInstanceId}/{@code fenceToken}/{@code leaseExpiresTs}
 * plus acquisition/loss evidence). {@code epoch} is the gate-generation value;
 * {@code fenceToken} is the per-partition owner sequence that must still be
 * valid immediately before every authorized bridge command.
 *
 * <p>Validation helpers here are pure and shared by the offline engine and its
 * tests, so "reject stale lease/fence/epoch immediately before the bridge
 * command" has a single implementation.
 */
public record GateRow(
        String partitionId,
        String accountScopeId,
        GateState state,
        long epoch,
        String reason,
        String evidenceHash,
        String approval1,
        String approval2,
        String approvedEvidenceHash,
        String ownerInstanceId,
        long fenceToken,
        Long fenceAcquiredTs,
        Long leaseExpiresTs,
        Long fenceLostTs) {

    public GateRow {
        Objects.requireNonNull(partitionId, "partitionId");
        Objects.requireNonNull(accountScopeId, "accountScopeId");
        Objects.requireNonNull(state, "state");
    }

    /** Single-operator approval (Saurabh) is present and covers an evidence hash (DEC-044). */
    public boolean approvalsComplete() {
        return approval1 != null && approvedEvidenceHash != null;
    }

    /** Whether the approvals covered the given evidence hash exactly. */
    public boolean approvalsCover(String hash) {
        return approvedEvidenceHash != null && approvedEvidenceHash.equals(hash);
    }

    /** Whether the fence is live: this owner holds it, the token is current, the lease is unexpired. */
    public boolean fenceValidFor(String owner, long token, long nowTs) {
        if (ownerInstanceId == null) {
            return false; // no lease acquired — fencing not established
        }
        if (!ownerInstanceId.equals(owner)) {
            return false; // a different (stale) instance claims to own the partition
        }
        if (token < fenceToken) {
            return false; // monotonically increasing sequence: a lower token is a stale owner
        }
        if (leaseExpiresTs != null && nowTs > leaseExpiresTs) {
            return false; // lease expired — fail closed on lease loss
        }
        return true;
    }

    /** Whether the current lease has expired at {@code nowTs} (used to detect lease loss). */
    public boolean fenceExpiredAt(long nowTs) {
        return leaseExpiresTs != null && nowTs > leaseExpiresTs;
    }

    /** Copy with a new gate state and a freshly incremented epoch (transition_ts). */
    public GateRow withState(GateState newState, String newReason, String hash, long ts) {
        return new GateRow(partitionId, accountScopeId, newState, epoch + 1, newReason, hash,
                approval1, approval2, approvedEvidenceHash, ownerInstanceId, fenceToken,
                fenceAcquiredTs, leaseExpiresTs, fenceLostTs);
    }

    /** Copy that records a fence acquisition (owner, monotonic token, lease horizon, acquired ts). */
    public GateRow withFence(String owner, long newToken, long acquiredTs, long leaseMs) {
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, owner, newToken, acquiredTs,
                acquiredTs + leaseMs, null);
    }

    /** Copy that records fence loss (lease lost) and returns the gate to a halted/failed-closed state. */
    public GateRow withFenceLost(long lostTs) {
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, ownerInstanceId, fenceToken,
                fenceAcquiredTs, leaseExpiresTs, lostTs);
    }

    /** Copy that extends the lease for the current holder without changing fenceToken. */
    public GateRow withRenewedLease(long nowTs, long leaseMs) {
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, ownerInstanceId, fenceToken,
                fenceAcquiredTs, nowTs + leaseMs, null);
    }

    /** Copy that clears the fence (owner, token, lease) and records revocation/loss at clearedTs. */
    public GateRow withFenceCleared(long clearedTs) {
        if (ownerInstanceId == null && fenceToken == 0L && fenceAcquiredTs == null && leaseExpiresTs == null) {
            return this;
        }
        return new GateRow(partitionId, accountScopeId, state, epoch, reason, evidenceHash,
                approval1, approval2, approvedEvidenceHash, null, 0L, null, null, clearedTs);
    }
}
