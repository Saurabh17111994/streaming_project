package com.trading.common.invariants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.invariants.LiveMoneyGuard.LiveMoneyFacts;
import com.trading.common.invariants.LiveMoneyGuard.LiveMoneyReadiness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-041/R-072/R-181 — the live-money guard now lives in the compiled module
 * (src/main/java), the builder requires every one of the ten conditions to be
 * set explicitly (no silent "all false" approval), and evaluate() null-guards.
 */
@DisplayName("R-041: LiveMoneyGuard lives in the compiled module and is fail-closed")
class LiveMoneyGuardTest {

    private static LiveMoneyFacts allClear() {
        return LiveMoneyFacts.builder()
                .criticalRiskOpen(false)
                .brokerIdentityUnverified(false)
                .flussFlinkCapabilityUnverified(false)
                .ddlRequirementsDisagree(false)
                .executorStateInvalid(false)
                .attemptOutcomeUnresolved(false)
                .changelogCheckpointUnknown(false)
                .safeHaltResumeUnproven(false)
                .observabilityUnavailable(false)
                .eodAuditRetentionUnverified(false)
                .build();
    }

    @Test
    @DisplayName("all-clear facts approve live money")
    void allClearApproves() {
        LiveMoneyReadiness r = LiveMoneyGuard.evaluate(allClear());
        assertTrue(r.liveMoneyAllowed);
        assertTrue(r.triggeredConditions.isEmpty());
        assertEquals("ok", r.haltReason());
    }

    @Test
    @DisplayName("any triggered condition halts live money")
    void anyTriggeredHalts() {
        LiveMoneyFacts facts = LiveMoneyFacts.builder()
                .criticalRiskOpen(false)
                .brokerIdentityUnverified(true)  // triggered
                .flussFlinkCapabilityUnverified(false)
                .ddlRequirementsDisagree(false)
                .executorStateInvalid(false)
                .attemptOutcomeUnresolved(false)
                .changelogCheckpointUnknown(false)
                .safeHaltResumeUnproven(false)
                .observabilityUnavailable(false)
                .eodAuditRetentionUnverified(false)
                .build();
        LiveMoneyReadiness r = LiveMoneyGuard.evaluate(facts);
        assertFalse(r.liveMoneyAllowed);
        assertTrue(r.triggeredConditions.contains(
                LiveMoneyStopCondition.BROKER_IDENTITY_UNVERIFIED));
        assertTrue(r.haltReason().contains("broker/protocol identity"));
    }

    @Test
    @DisplayName("omitting any of the ten conditions fails the build (R-072)")
    void omittedConditionFailsBuild() {
        LiveMoneyFacts.Builder b = LiveMoneyFacts.builder()
                .criticalRiskOpen(false)
                .brokerIdentityUnverified(false)
                .flussFlinkCapabilityUnverified(false)
                .ddlRequirementsDisagree(false)
                .executorStateInvalid(false)
                .attemptOutcomeUnresolved(false)
                .changelogCheckpointUnknown(false)
                .safeHaltResumeUnproven(false)
                .observabilityUnavailable(false);
        // eodAuditRetentionUnverified never set → build must fail, never
        // silently approve live money with a defaulted false.
        IllegalStateException e = assertThrows(IllegalStateException.class, b::build);
        assertTrue(e.getMessage().contains("eodAuditRetentionUnverified"),
                e.getMessage());
    }

    @Test
    @DisplayName("evaluate null-guards facts (R-181)")
    void evaluateNullGuards() {
        assertThrows(NullPointerException.class, () -> LiveMoneyGuard.evaluate(null));
    }
}
