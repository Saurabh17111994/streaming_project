package common.invariants;

/**
 * The ten documented live-money stop conditions (01-foundation.md,
 * "Live-money stop conditions"). Live-money order placement must remain
 * DISABLED while any of these is true.
 *
 * The guard is intentionally dependency-free: it only knows the condition
 * vocabulary and an evaluator. The surrounding runtime (executor / coordinator
 * / CI) supplies the actual facts.
 */
public enum LiveMoneyStopCondition {
    CRITICAL_RISK_OPEN("A critical risk is open"),
    BROKER_IDENTITY_UNVERIFIED(
            "A broker/protocol identity or response behavior is unverified"),
    FLUSS_FLINK_CAPABILITY_UNVERIFIED(
            "A Fluss/Flink capability is assumed but not version-tested"),
    DDL_REQUIREMENTS_DISAGREE("DDL and requirements disagree"),
    EXECUTOR_STATE_INVALID(
            "Executor state is missing, corrupt, unfenced, or not auditable"),
    ATTEMPT_OUTCOME_UNRESOLVED("An attempt has an unresolved outcome"),
    CHANGELOG_CHECKPOINT_UNKNOWN(
            "Changelog continuity or checkpoint health is unknown"),
    SAFE_HALT_RESUME_UNPROVEN("Safe-halt or two-person resume is unproven"),
    OBSERVABILITY_UNAVAILABLE("Required observability is unavailable"),
    EOD_AUDIT_RETENTION_UNVERIFIED(
            "EOD data or audit retention is unverified");

    private final String description;

    LiveMoneyStopCondition(String description) {
        this.description = description;
    }

    /** Human-readable description used in halt records and alerts. */
    public String getDescription() {
        return description;
    }

    /** Stable id used in audit / halt records. */
    public String conditionId() {
        return name();
    }
}
