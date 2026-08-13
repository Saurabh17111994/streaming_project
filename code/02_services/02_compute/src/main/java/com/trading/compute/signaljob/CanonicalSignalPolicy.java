package com.trading.compute.signaljob;

/**
 * Canonical-signal policy (DEC-035, tracker 14 re-scoped P2 —
 * SIGNAL-SCHEMA-001).
 *
 * <p>A signal row is <b>canonical</b> iff all four identity columns equal the
 * expected values exactly: {@code schema_version == expectedSchemaVersion}
 * <em>and</em> {@code strategy_id == expectedStrategyId} <em>and</em>
 * {@code strategy_version == expectedStrategyVersion} <em>and</em>
 * {@code rule_id == expectedRuleId}. Any other combination — a strategy
 * iteration, an unversioned row, a blank value — is non-canonical and is
 * excluded from the canonical current-state projection
 * ({@code Signal_Candidates_current}). The LOG twin
 * ({@code Signal_Candidates}) keeps every emitted signal regardless, so no
 * audit history is lost by the filter.
 *
 * <p>The expected identity is {@link SignalCandidatesTableColumns}' pinned
 * canonical constants (the config defaults ARE the canonical identity); an
 * overridden {@code SIGNAL_STRATEGY_*} config therefore feeds rows that this
 * policy filters from the KV projection and counts via
 * {@code compute.signal.kv.filtered.noncanonical}.
 *
 * <p>Strict exact-match semantics: {@code null} and blank values never match,
 * and whitespace is not trimmed (a padded value is a different identity
 * string, not an equivalent one).
 */
public final class CanonicalSignalPolicy {

    private CanonicalSignalPolicy() {}

    /**
     * @param schemaVersion            the row's {@code schema_version}
     * @param strategyId               the row's {@code strategy_id}
     * @param strategyVersion          the row's {@code strategy_version}
     * @param ruleId                   the row's {@code rule_id}
     * @param expectedSchemaVersion    the pinned expected schema version
     * @param expectedStrategyId       the pinned expected strategy id
     * @param expectedStrategyVersion  the pinned expected strategy version
     * @param expectedRuleId           the pinned expected rule id
     * @return true iff all four row identity columns exactly equal the expected values
     */
    public static boolean isCanonical(
            String schemaVersion,
            String strategyId,
            String strategyVersion,
            String ruleId,
            String expectedSchemaVersion,
            String expectedStrategyId,
            String expectedStrategyVersion,
            String expectedRuleId) {
        return expectedSchemaVersion != null
                && expectedSchemaVersion.equals(schemaVersion)
                && expectedStrategyId != null
                && expectedStrategyId.equals(strategyId)
                && expectedStrategyVersion != null
                && expectedStrategyVersion.equals(strategyVersion)
                && expectedRuleId != null
                && expectedRuleId.equals(ruleId);
    }
}
