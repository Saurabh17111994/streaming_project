package com.trading.common.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.ImmutabilityProtocol;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

/** Unit tests for the single-operator (Saurabh) audit deletion governance path (DEC-044). */
class AuditDeletionControlTest {

    private static final long TS = 1_700_000_000_000L;

    private static AuditDeletionControl.DeletionContext context() {
        return new AuditDeletionControl.DeletionContext(
                Set.of("RPC-42"), Set.of("LHR-7"), Set.of("saurabh"));
    }

    private static AuditDeletionControl.DeletionRequest request(boolean withinWindow,
                                                                String policyChange,
                                                                String legalHold,
                                                                List<String> authorizers) {
        return new AuditDeletionControl.DeletionRequest("del-1", "Execution_Audit:2024-06-01",
                withinWindow, policyChange, legalHold, authorizers);
    }

    private static AuditDeletionControl.DeletionRequest fullRequest() {
        return request(true, "RPC-42", "LHR-7", List.of("saurabh"));
    }

    @Test
    void approvedWithFullEvidence() {
        AuditDeletionControl.DeletionDecision d =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.APPROVED);
        assertThat(d.evidenceEvent()).isNotNull();
        assertThat(d.evidenceEvent().approved()).isTrue();
        assertThat(d.evidenceEvent().authorizerOne()).isNotBlank();
        assertThat(d.evidenceEvent().authorizerOne()).isEqualTo("saurabh");
    }

    @Test
    void rejectedWithoutApprovedPolicyChange() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-999", "LHR-7", List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_NO_POLICY_CHANGE);
    }

    @Test
    void rejectedWhileLegalHoldActive() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-0", List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_LEGAL_HOLD);
    }

    @Test
    void rejectedWithNoAuthorizer() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of()), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER);
    }

    @Test
    void rejectedWithTwoAuthorizersWhenOneRequired() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of("saurabh", "ops-2")), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER);
    }

    @Test
    void rejectedWhenOperatorNotAuthorized() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of("intruder")), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_UNAUTHORIZED_OPERATOR);
    }

    @Test
    void rejectedWhenRequestMissingEvidence() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                new AuditDeletionControl.DeletionRequest("", "Execution_Audit:2024-06-01",
                        true, "RPC-42", "LHR-7", List.of("saurabh")),
                context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_MISSING_EVIDENCE);
    }

    @Test
    void approvedOutsideRetentionWindowWithoutPolicyChange() {
        // Records past the approved retention window: normal expiry, no policy-change or
        // legal-hold evidence required — single-operator authorization still applies.
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(false, null, null, List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.APPROVED);
        assertThat(d.evidenceEvent().withinRetentionWindow()).isFalse();
    }

    @Test
    void evidenceIsDeterministicAndImmutable() {
        AuditDeletionControl.DeletionEvidenceEvent a =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent b =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        assertThat(a.eventId()).isEqualTo(b.eventId());
        assertThat(a.contentHash()).isEqualTo(b.contentHash());
        assertThat(a.eventId()).hasSize(AuditHashChain.HASH_HEX_LENGTH);
        assertThat(a.contentHash()).hasSize(AuditHashChain.HASH_HEX_LENGTH);
    }

    @Test
    void replayOfApprovedEventIsDuplicate() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        assertThat(AuditDeletionControl.classify(ev, ev))
                .isEqualTo(ImmutabilityProtocol.Outcome.DUPLICATE);
    }

    @Test
    void mutatedEvidenceIsViolation() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        // Same event identity, different content hash — mutation of immutable evidence.
        AuditDeletionControl.DeletionEvidenceEvent mutated =
                new AuditDeletionControl.DeletionEvidenceEvent(
                        ev.eventId(), ev.requestId(), ev.scope(), ev.approved(),
                        ev.withinRetentionWindow(), ev.retentionPolicyChangeId(),
                        ev.legalHoldReleaseId(), ev.authorizerOne(), ev.authorizerTwo(),
                        ev.timestampMs(), "0".repeat(AuditHashChain.HASH_HEX_LENGTH));
        assertThat(AuditDeletionControl.classify(ev, mutated))
                .isEqualTo(ImmutabilityProtocol.Outcome.VIOLATION);
    }

    @Test
    void distinctRequestIsAcceptedNotDuplicate() {
        AuditDeletionControl.DeletionEvidenceEvent a =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent b =
                AuditDeletionControl.evaluate(
                        new AuditDeletionControl.DeletionRequest("del-2", "Execution_Audit:2024-06-01",
                                true, "RPC-42", "LHR-7", List.of("saurabh")),
                        context(), TS).evidenceEvent();
        assertThat(AuditDeletionControl.classify(a, b))
                .isEqualTo(ImmutabilityProtocol.Outcome.ACCEPTED);
    }

    @Test
    void evidenceFeedsAuditHashChain() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditHashChain.Manifest manifest =
                new AuditHashChain.ManifestBuilder("2025-01-01", "Execution_Audit", "1")
                        .addEvent(ev.asAuditEvent().eventId(), ev.asAuditEvent().contentHash())
                        .build();
        assertThat(AuditHashChain.verifyManifestAgainstSource(manifest, manifest.events()))
                .isEqualTo(AuditHashChain.Verification.VALID);
    }

    @Test
    void rejectedAttemptStillEmitsEvidenceEvent() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-999", "LHR-7", List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_NO_POLICY_CHANGE);
        assertThat(d.evidenceEvent()).isNotNull();
        assertThat(d.evidenceEvent().approved()).isFalse();
        assertThat(d.evidenceEvent().eventId()).isNotBlank();
    }
}
