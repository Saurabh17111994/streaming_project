package common.invariants;

import java.util.EnumSet;
import java.util.Set;

/**
 * Evaluates a live-money readiness snapshot against the ten stop conditions
 * and reports whether live-money placement is allowed.
 *
 * The caller supplies a {@link LiveMoneyFacts} snapshot: one boolean per
 * condition (true = that stop condition is currently triggered). Keeping the
 * facts as plain booleans means the guard has no external dependencies; the
 * runtime that knows the real state builds the snapshot.
 */
public final class LiveMoneyGuard {

    private LiveMoneyGuard() {
    }

    /** Immutable snapshot of the ten stop-condition triggers. */
    public static final class LiveMoneyFacts {
        public final boolean criticalRiskOpen;
        public final boolean brokerIdentityUnverified;
        public final boolean flussFlinkCapabilityUnverified;
        public final boolean ddlRequirementsDisagree;
        public final boolean executorStateInvalid;
        public final boolean attemptOutcomeUnresolved;
        public final boolean changelogCheckpointUnknown;
        public final boolean safeHaltResumeUnproven;
        public final boolean observabilityUnavailable;
        public final boolean eodAuditRetentionUnverified;

        private LiveMoneyFacts(
                boolean criticalRiskOpen,
                boolean brokerIdentityUnverified,
                boolean flussFlinkCapabilityUnverified,
                boolean ddlRequirementsDisagree,
                boolean executorStateInvalid,
                boolean attemptOutcomeUnresolved,
                boolean changelogCheckpointUnknown,
                boolean safeHaltResumeUnproven,
                boolean observabilityUnavailable,
                boolean eodAuditRetentionUnverified) {
            this.criticalRiskOpen = criticalRiskOpen;
            this.brokerIdentityUnverified = brokerIdentityUnverified;
            this.flussFlinkCapabilityUnverified = flussFlinkCapabilityUnverified;
            this.ddlRequirementsDisagree = ddlRequirementsDisagree;
            this.executorStateInvalid = executorStateInvalid;
            this.attemptOutcomeUnresolved = attemptOutcomeUnresolved;
            this.changelogCheckpointUnknown = changelogCheckpointUnknown;
            this.safeHaltResumeUnproven = safeHaltResumeUnproven;
            this.observabilityUnavailable = observabilityUnavailable;
            this.eodAuditRetentionUnverified = eodAuditRetentionUnverified;
        }

        /** Returns the set of stop conditions currently triggered. */
        public Set<LiveMoneyStopCondition> triggered() {
            Set<LiveMoneyStopCondition> s =
                    EnumSet.noneOf(LiveMoneyStopCondition.class);
            if (criticalRiskOpen) {
                s.add(LiveMoneyStopCondition.CRITICAL_RISK_OPEN);
            }
            if (brokerIdentityUnverified) {
                s.add(LiveMoneyStopCondition.BROKER_IDENTITY_UNVERIFIED);
            }
            if (flussFlinkCapabilityUnverified) {
                s.add(LiveMoneyStopCondition.FLUSS_FLINK_CAPABILITY_UNVERIFIED);
            }
            if (ddlRequirementsDisagree) {
                s.add(LiveMoneyStopCondition.DDL_REQUIREMENTS_DISAGREE);
            }
            if (executorStateInvalid) {
                s.add(LiveMoneyStopCondition.EXECUTOR_STATE_INVALID);
            }
            if (attemptOutcomeUnresolved) {
                s.add(LiveMoneyStopCondition.ATTEMPT_OUTCOME_UNRESOLVED);
            }
            if (changelogCheckpointUnknown) {
                s.add(LiveMoneyStopCondition.CHANGELOG_CHECKPOINT_UNKNOWN);
            }
            if (safeHaltResumeUnproven) {
                s.add(LiveMoneyStopCondition.SAFE_HALT_RESUME_UNPROVEN);
            }
            if (observabilityUnavailable) {
                s.add(LiveMoneyStopCondition.OBSERVABILITY_UNAVAILABLE);
            }
            if (eodAuditRetentionUnverified) {
                s.add(LiveMoneyStopCondition.EOD_AUDIT_RETENTION_UNVERIFIED);
            }
            return s;
        }

        /** Fluent builder for constructing a facts snapshot. */
        public static Builder builder() {
            return new Builder();
        }

        /** Builder for {@link LiveMoneyFacts}. */
        public static final class Builder {
            private boolean criticalRiskOpen;
            private boolean brokerIdentityUnverified;
            private boolean flussFlinkCapabilityUnverified;
            private boolean ddlRequirementsDisagree;
            private boolean executorStateInvalid;
            private boolean attemptOutcomeUnresolved;
            private boolean changelogCheckpointUnknown;
            private boolean safeHaltResumeUnproven;
            private boolean observabilityUnavailable;
            private boolean eodAuditRetentionUnverified;

            public Builder criticalRiskOpen(boolean v) {
                this.criticalRiskOpen = v;
                return this;
            }

            public Builder brokerIdentityUnverified(boolean v) {
                this.brokerIdentityUnverified = v;
                return this;
            }

            public Builder flussFlinkCapabilityUnverified(boolean v) {
                this.flussFlinkCapabilityUnverified = v;
                return this;
            }

            public Builder ddlRequirementsDisagree(boolean v) {
                this.ddlRequirementsDisagree = v;
                return this;
            }

            public Builder executorStateInvalid(boolean v) {
                this.executorStateInvalid = v;
                return this;
            }

            public Builder attemptOutcomeUnresolved(boolean v) {
                this.attemptOutcomeUnresolved = v;
                return this;
            }

            public Builder changelogCheckpointUnknown(boolean v) {
                this.changelogCheckpointUnknown = v;
                return this;
            }

            public Builder safeHaltResumeUnproven(boolean v) {
                this.safeHaltResumeUnproven = v;
                return this;
            }

            public Builder observabilityUnavailable(boolean v) {
                this.observabilityUnavailable = v;
                return this;
            }

            public Builder eodAuditRetentionUnverified(boolean v) {
                this.eodAuditRetentionUnverified = v;
                return this;
            }

            public LiveMoneyFacts build() {
                return new LiveMoneyFacts(
                        criticalRiskOpen,
                        brokerIdentityUnverified,
                        flussFlinkCapabilityUnverified,
                        ddlRequirementsDisagree,
                        executorStateInvalid,
                        attemptOutcomeUnresolved,
                        changelogCheckpointUnknown,
                        safeHaltResumeUnproven,
                        observabilityUnavailable,
                        eodAuditRetentionUnverified);
            }
        }
    }

    /**
     * Evaluates the snapshot. Live-money is allowed only when no stop
     * condition is triggered.
     */
    public static LiveMoneyReadiness evaluate(LiveMoneyFacts facts) {
        Set<LiveMoneyStopCondition> triggered = facts.triggered();
        return new LiveMoneyReadiness(triggered.isEmpty(), triggered);
    }

    /** Result of a live-money readiness evaluation. */
    public static final class LiveMoneyReadiness {
        public final boolean liveMoneyAllowed;
        public final Set<LiveMoneyStopCondition> triggeredConditions;

        private LiveMoneyReadiness(
                boolean liveMoneyAllowed,
                Set<LiveMoneyStopCondition> triggeredConditions) {
            this.liveMoneyAllowed = liveMoneyAllowed;
            this.triggeredConditions = triggeredConditions;
        }

        /** Human-readable halt reason joining all triggered descriptions. */
        public String haltReason() {
            if (liveMoneyAllowed) {
                return "ok";
            }
            StringBuilder sb = new StringBuilder("live-money halted: ");
            boolean first = true;
            for (LiveMoneyStopCondition c : triggeredConditions) {
                if (!first) {
                    sb.append("; ");
                }
                sb.append(c.getDescription());
                first = false;
            }
            return sb.toString();
        }
    }
}
