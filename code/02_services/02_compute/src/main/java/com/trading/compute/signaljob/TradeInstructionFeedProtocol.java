package com.trading.compute.signaljob;

import com.trading.common.schema.ImmutabilityProtocol;

/**
 * Immutable instruction-feed protocol (SCH-19; REQ-FLS-008/015, REQ-SS-004,
 * {@code docs/04_contracts/07-executor.md}).
 *
 * <p>Per {@code instruction_id} the writer persists the canonical content hash
 * (see {@link TradeDecisionBuilder#canonicalHash}) in the instruction index
 * (the Fluss KV twin, DEC-038 — Fluss is the authoritative durable index; the
 * LOG itself is the rebuild source). Every emission is checked against that
 * stored hash before the LOG append:
 *
 * <pre>{@code
 * no stored hash        → ACCEPTED  (first write: append LOG + record hash)
 * stored hash == new    → DUPLICATE (idempotent replay/restart re-emission: drop)
 * stored hash != new    → VIOLATION (contract violation: quarantine + halt,
 *                                    original row is never mutated)
 * }</pre>
 *
 * <p>This is the LOG-table realization of the canonical immutability protocol
 * ({@link ImmutabilityProtocol}: same id + same hash = duplicate evidence;
 * same id + different hash = contract violation). LOG tables have no
 * point-lookup by key, so the writer MUST persist/query enough state to detect
 * mutation — LOG comments or {@code NOT ENFORCED} keys do not enforce it.
 *
 * <p>Pure JVM: no state, no side effects. On {@code VIOLATION} the caller
 * (the future decision operator) raises the {@link EnforcementViolation}
 * quarantine event and the halt path ({@code Safety_Halt_Requests} + live-money
 * stop condition), per REQ-FLS-015 — the original immutable row is never
 * touched.
 */
public final class TradeInstructionFeedProtocol {

    private TradeInstructionFeedProtocol() {}

    /** Result of checking one emission against the stored instruction hash. */
    public record Verification(
            ImmutabilityProtocol.Outcome outcome,
            String instructionId,
            String contentHash,
            long timestampMs) {

        public boolean accepted() {
            return outcome == ImmutabilityProtocol.Outcome.ACCEPTED;
        }

        public boolean duplicate() {
            return outcome == ImmutabilityProtocol.Outcome.DUPLICATE;
        }

        public boolean violation() {
            return outcome == ImmutabilityProtocol.Outcome.VIOLATION;
        }
    }

    /**
     * REQ-FLS-015 quarantine enforcement event: a separate, identifiable
     * record of a violation carrying source identity, content hash, and
     * timestamp — never a mutation of the original instruction row.
     */
    public record EnforcementViolation(String instructionId, String contentHash,
                                       long timestampMs) {}

    /**
     * Evaluate one emission against the stored hash for its
     * {@code instructionId}. {@code existingHash} is {@code null} on first
     * write (never stored). Pure: the caller decides what to persist from the
     * outcome — this method never writes.
     */
    public static Verification verify(String instructionId, String existingHash,
                                      String incomingHash, long timestampMs) {
        ImmutabilityProtocol.Outcome outcome =
                ImmutabilityProtocol.evaluate(existingHash, incomingHash);
        return new Verification(outcome, instructionId, incomingHash, timestampMs);
    }

    /**
     * The quarantine + halt signal: only a VIOLATION requires it — an
     * accepted first write and an idempotent duplicate are both clean.
     */
    public static boolean requiresHalt(Verification verification) {
        return verification.violation();
    }

    /**
     * Build the REQ-FLS-015 enforcement event for a VIOLATION. Refuses to
     * fabricate an event for a clean outcome — the caller must not quarantine
     * a duplicate or a first write.
     */
    public static EnforcementViolation enforcementEvent(Verification verification) {
        if (!verification.violation()) {
            throw new IllegalStateException(
                    "enforcement event only for VIOLATION, got " + verification.outcome()
                            + " for instruction " + verification.instructionId());
        }
        return new EnforcementViolation(
                verification.instructionId(), verification.contentHash(), verification.timestampMs());
    }
}
