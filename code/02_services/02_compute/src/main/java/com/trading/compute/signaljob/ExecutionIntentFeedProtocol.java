package com.trading.compute.signaljob;

import com.trading.common.schema.ImmutabilityProtocol;

/** Pure duplicate and changed-content protocol for the immutable intent LOG. */
public final class ExecutionIntentFeedProtocol {

    private ExecutionIntentFeedProtocol() {}

    public record Verification(
            ImmutabilityProtocol.Outcome outcome,
            String instructionId,
            String requestHash,
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

    public record EnforcementViolation(String instructionId, String requestHash, long timestampMs) {}

    public static Verification verify(String instructionId, String existingHash,
            String incomingHash, long timestampMs) {
        if (instructionId == null || instructionId.isBlank()
                || incomingHash == null || incomingHash.isBlank()) {
            throw new IllegalArgumentException("instruction_id and request_hash are required");
        }
        return new Verification(ImmutabilityProtocol.evaluate(existingHash, incomingHash),
                instructionId, incomingHash, timestampMs);
    }

    public static boolean requiresHalt(Verification verification) {
        return verification.violation();
    }

    public static EnforcementViolation enforcementEvent(Verification verification) {
        if (!verification.violation()) {
            throw new IllegalStateException("enforcement event requires VIOLATION");
        }
        return new EnforcementViolation(verification.instructionId(), verification.requestHash(),
                verification.timestampMs());
    }
}
