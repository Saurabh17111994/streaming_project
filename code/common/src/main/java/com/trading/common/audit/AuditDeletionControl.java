package com.trading.common.audit;

import com.trading.common.schema.ImmutabilityProtocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Approved audit-retention deletion governance
 * (docs/02_requirements/03-non-functional.md &sect;3.4.1 "Deletion";
 * docs/02_requirements/04-data.md: deletion of audit records before the approved retention period
 * is prohibited unless an approved retention-policy change, a legal-hold release,
 * and single-operator (Saurabh) authorization are recorded as immutable deletion-evidence events).
 * <p>DEC-044 (2026-08-21): single-operator Saurabh (was two-person).
 */
public final class AuditDeletionControl {

    private AuditDeletionControl() {}

    public enum Decision {
        APPROVED,
        REJECTED_NO_POLICY_CHANGE,
        REJECTED_LEGAL_HOLD,
        REJECTED_REQUIRES_SINGLE_AUTHORIZER,
        REJECTED_UNAUTHORIZED_OPERATOR,
        REJECTED_MISSING_EVIDENCE
    }

    /** A deletion attempt against the policy-controlled audit store. */
    public record DeletionRequest(
            String requestId,
            String scope,
            boolean withinRetentionWindow,
            String retentionPolicyChangeId,
            String legalHoldReleaseId,
            List<String> authorizers) {
        public DeletionRequest {
            authorizers = authorizers == null ? List.of() : List.copyOf(authorizers);
        }
    }

    /** What the caller knows about approved policy changes, legal holds, and operators. */
    public record DeletionContext(
            Set<String> approvedPolicyChangeIds,
            Set<String> legalHoldReleaseIds,
            Set<String> authorizedOperators) {
        public DeletionContext {
            approvedPolicyChangeIds =
                    approvedPolicyChangeIds == null ? Set.of() : Set.copyOf(approvedPolicyChangeIds);
            legalHoldReleaseIds =
                    legalHoldReleaseIds == null ? Set.of() : Set.copyOf(legalHoldReleaseIds);
            authorizedOperators =
                    authorizedOperators == null ? Set.of() : Set.copyOf(authorizedOperators);
        }
    }

    /**
     * Immutable evidence of one deletion attempt. Approved and rejected attempts
     * both produce an event; each event feeds the immutable audit hash chain
     * via {@link #asAuditEvent()}.
     */
    public record DeletionEvidenceEvent(
            String eventId,
            String requestId,
            String scope,
            boolean approved,
            boolean withinRetentionWindow,
            String retentionPolicyChangeId,
            String legalHoldReleaseId,
            String authorizerOne,
            String authorizerTwo,
            long timestampMs,
            String contentHash) {

        public DeletionEvidenceEvent {
            if (eventId == null || eventId.isBlank()) {
                throw new IllegalArgumentException("eventId must be non-blank");
            }
            if (contentHash == null || contentHash.isBlank()) {
                throw new IllegalArgumentException("contentHash must be non-blank");
            }
        }

        public String canonical() {
            return canonicalForm(requestId, scope, approved, withinRetentionWindow,
                    retentionPolicyChangeId, legalHoldReleaseId,
                    authorizerOne, authorizerTwo, timestampMs);
        }

        /** The evidence identity feeds the immutable audit hash chain. */
        public AuditHashChain.AuditEvent asAuditEvent() {
            return new AuditHashChain.AuditEvent(eventId, contentHash);
        }
    }

    public record DeletionDecision(Decision decision, DeletionEvidenceEvent evidenceEvent) {}

    /**
     * Evaluates a deletion request against the known governance state. Approval
     * requires a single authorized operator (Saurabh, DEC-044). Deletion of records
     * still inside the approved retention window additionally requires an
     * approved retention-policy change and a legal-hold release. Every attempt —
     * approved or rejected — produces an immutable deletion-evidence event.
     */
    public static DeletionDecision evaluate(DeletionRequest request,
                                            DeletionContext context,
                                            long timestampMs) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(context, "context");
        if (request.requestId() == null || request.requestId().isBlank()
                || request.scope() == null || request.scope().isBlank()) {
            return new DeletionDecision(Decision.REJECTED_MISSING_EVIDENCE,
                    buildEvent(request, false, null, null, timestampMs));
        }
        Set<String> distinctAuthorizers = new LinkedHashSet<>(request.authorizers());
        if (distinctAuthorizers.size() != 1) {
            return new DeletionDecision(Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER,
                    buildEvent(request, false, null, null, timestampMs));
        }
        for (String operator : distinctAuthorizers) {
            if (!context.authorizedOperators().contains(operator)) {
                return new DeletionDecision(Decision.REJECTED_UNAUTHORIZED_OPERATOR,
                        buildEvent(request, false, null, null, timestampMs));
            }
        }
        if (request.withinRetentionWindow()) {
            if (request.retentionPolicyChangeId() == null
                    || !context.approvedPolicyChangeIds().contains(request.retentionPolicyChangeId())) {
                return new DeletionDecision(Decision.REJECTED_NO_POLICY_CHANGE,
                        buildEvent(request, false, null, null, timestampMs));
            }
            if (request.legalHoldReleaseId() == null
                    || !context.legalHoldReleaseIds().contains(request.legalHoldReleaseId())) {
                return new DeletionDecision(Decision.REJECTED_LEGAL_HOLD,
                        buildEvent(request, false, null, null, timestampMs));
            }
        }
        String single = distinctAuthorizers.iterator().next();
        DeletionEvidenceEvent evidence =
                buildEvent(request, true, single, null, timestampMs);
        return new DeletionDecision(Decision.APPROVED, evidence);
    }

    /**
     * Same eventId + same contentHash &rarr; DUPLICATE (idempotent replay of the
     * same evidence); same eventId + different contentHash &rarr; VIOLATION
     * (mutation of immutable evidence). Different eventIds &rarr; ACCEPTED (a
     * distinct deletion event).
     */
    public static ImmutabilityProtocol.Outcome classify(DeletionEvidenceEvent existing,
                                                        DeletionEvidenceEvent incoming) {
        if (existing.eventId().equals(incoming.eventId())) {
            return existing.contentHash().equals(incoming.contentHash())
                    ? ImmutabilityProtocol.Outcome.DUPLICATE
                    : ImmutabilityProtocol.Outcome.VIOLATION;
        }
        return ImmutabilityProtocol.Outcome.ACCEPTED;
    }

    private static DeletionEvidenceEvent buildEvent(DeletionRequest request, boolean approved,
                                                    String authorizerOne, String authorizerTwo,
                                                    long timestampMs) {
        String canonical = canonicalForm(request.requestId(), request.scope(), approved,
                request.withinRetentionWindow(), request.retentionPolicyChangeId(),
                request.legalHoldReleaseId(), authorizerOne, authorizerTwo, timestampMs);
        String contentHash = ImmutabilityProtocol.canonicalHash(canonical);
        String eventId = ImmutabilityProtocol.canonicalHash("deletion-evidence-v1|" + canonical);
        return new DeletionEvidenceEvent(eventId, request.requestId(), request.scope(), approved,
                request.withinRetentionWindow(), request.retentionPolicyChangeId(),
                request.legalHoldReleaseId(), authorizerOne, authorizerTwo, timestampMs, contentHash);
    }

    private static String canonicalForm(String requestId, String scope, boolean approved,
                                        boolean withinWindow, String policyChange,
                                        String legalHoldRelease, String authorizerOne,
                                        String authorizerTwo, long timestampMs) {
        return "deletion-evidence-v1\n"
                + "requestId=" + requestId + "\n"
                + "scope=" + scope + "\n"
                + "approved=" + approved + "\n"
                + "withinRetentionWindow=" + withinWindow + "\n"
                + "policyChange=" + (policyChange == null ? "" : policyChange) + "\n"
                + "legalHoldRelease=" + (legalHoldRelease == null ? "" : legalHoldRelease) + "\n"
                + "authorizerOne=" + (authorizerOne == null ? "" : authorizerOne) + "\n"
                + "authorizerTwo=" + (authorizerTwo == null ? "" : authorizerTwo) + "\n"
                + "timestampMs=" + timestampMs;
    }
}
