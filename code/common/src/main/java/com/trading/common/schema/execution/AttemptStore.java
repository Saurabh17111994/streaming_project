package com.trading.common.schema.execution;

/**
 * Attempt lifecycle store (docs/08_implementation/07-executor.md &rarr;
 * "Attempt rules"): mints exactly one PREPARED attempt per
 * (instruction_id, request_hash) and refuses to rewrite an attempt's identity.
 *
 * <p>The {@code prepare} contract, per the dossier:
 * <ul>
 *   <li>a new (instruction_id, request_hash) mints one PREPARED attempt
 *       ({@link AttemptRecord#prepared});</li>
 *   <li>a duplicate (instruction_id, request_hash) returns the existing attempt
 *       without creating a second record and without touching its identity;</li>
 *   <li>a modified decision under an existing instruction_id (same id, different
 *       request_hash) is a contract violation: the store requests a halt through
 *       its callback and does not mutate anything.</li>
 * </ul>
 */
public interface AttemptStore {

    enum Status {
        /** A new PREPARED attempt was minted. */
        CREATED,
        /** (instruction_id, request_hash) already exists — existing attempt returned. */
        DUPLICATE,
        /** Same instruction_id, different request_hash — identity cannot be rebound. */
        CONTRACT_VIOLATION
    }

    /** All identities and creation inputs are caller-supplied (deterministic). */
    record PrepareRequest(
            String executionAttemptId,
            String accountScopeId,
            String instructionId,
            String actionId,
            String executionPartitionId,
            String requestHash,
            String clientOrderRef,
            long gateEpoch,
            long nowTs) {}

    record PrepareResult(Status status, AttemptRecord record, String reason) {

        public static PrepareResult created(AttemptRecord record) {
            return new PrepareResult(Status.CREATED, record, null);
        }

        public static PrepareResult duplicate(AttemptRecord record) {
            return new PrepareResult(Status.DUPLICATE, record, "duplicate (instruction_id, request_hash)");
        }

        public static PrepareResult contractViolation(AttemptRecord record, String reason) {
            return new PrepareResult(Status.CONTRACT_VIOLATION, record, reason);
        }
    }

    PrepareResult prepare(PrepareRequest request);
}
