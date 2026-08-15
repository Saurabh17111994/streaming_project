package com.trading.common.schema.execution;

import com.trading.common.schema.ownership.ColumnOwnership;
import com.trading.common.schema.ownership.ExecutionAttemptsColumnOwnership;
import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory {@link AttemptStore} — the pure-JVM core of the executor attempt
 * lifecycle (docs/08_implementation/07-executor.md &rarr; Task 5), and the
 * first production consumer of the SCH-15 column-ownership guard
 * ({@link ColumnOwnership#checkWrite}).
 *
 * <p>{@link #prepare} is wired to the guard two ways:
 * <ul>
 *   <li><b>Store-level identity guard:</b> a duplicate (instruction_id,
 *       request_hash) returns the existing PREPARED attempt untouched; a
 *       modified decision under an existing instruction_id (same id, different
 *       hash) is a contract violation — the halt callback fires and no row is
 *       created or mutated. The identity columns of the PREPARED attempt are
 *       never rewritten after minting.</li>
 *   <li><b>Matrix self-assertion:</b> before accepting the minted row,
 *       {@code checkWrite(WRITER_ATTEMPT_STORE, phase, phase_epoch, prepared_ts,
 *       retry_attempt)} runs — if the ownership matrix ever drifts (those
 *       columns reassigned to identity or to the broker-adapter group), the
 *       store fails closed at the first write instead of silently writing
 *       columns it no longer owns.</li>
 * </ul>
 *
 * <p>Not thread-safe; single-writer executor. The Fluss-backed store and the
 * remaining Task 5 items (legal transitions, stale-epoch rejection, terminal
 * protection, UNKNOWN resolution) land with the {@code 04_executor} module.
 */
public final class InMemoryAttemptStore implements AttemptStore {

    private static final ColumnOwnership MATRIX = ExecutionAttemptsColumnOwnership.MATRIX;
    private static final String WRITER = ExecutionAttemptsColumnOwnership.WRITER_ATTEMPT_STORE;

    private final Map<String, AttemptRecord> byAttemptId = new LinkedHashMap<>();
    private final Map<String, String> instructionToAttemptId = new HashMap<>();
    private final Map<String, String> instructionHashToAttemptId = new HashMap<>();
    private final Runnable haltCallback;

    public InMemoryAttemptStore(Runnable haltCallback) {
        this.haltCallback = Objects.requireNonNull(haltCallback, "haltCallback");
    }

    private static String replayKey(String instructionId, String requestHash) {
        return instructionId + "\u0000" + requestHash;
    }

    @Override
    public PrepareResult prepare(PrepareRequest request) {
        Objects.requireNonNull(request, "request");
        String replayKey = replayKey(request.instructionId(), request.requestHash());
        String replayAttemptId = instructionHashToAttemptId.get(replayKey);
        if (replayAttemptId != null) {
            // Duplicate (instruction_id, request_hash): return the existing
            // PREPARED attempt untouched — identity is never rewritten.
            return PrepareResult.duplicate(byAttemptId.get(replayAttemptId));
        }
        String existingAttemptId = instructionToAttemptId.get(request.instructionId());
        if (existingAttemptId != null) {
            // Same instruction_id, different request_hash: a modified decision
            // under an existing instruction identity. Contract violation — halt
            // and do not mutate anything.
            haltCallback.run();
            return PrepareResult.contractViolation(byAttemptId.get(existingAttemptId),
                    "modified decision under existing instruction_id: " + request.instructionId());
        }

        AttemptRecord created = AttemptRecord.prepared(
                request.executionAttemptId(), request.accountScopeId(),
                request.instructionId(), request.actionId(),
                request.executionPartitionId(), request.requestHash(),
                request.clientOrderRef(), request.gateEpoch(), request.nowTs());

        // First consumer of the SCH-15 guard: the mutable group this store
        // writes must still belong to the attempt-store (and none of it be
        // identity) — fail closed on matrix drift before accepting the row.
        assertWritableColumns(ExecutionAttemptsColumns.PHASE,
                ExecutionAttemptsColumns.PHASE_EPOCH,
                ExecutionAttemptsColumns.PREPARED_TS,
                ExecutionAttemptsColumns.RETRY_ATTEMPT);

        byAttemptId.put(created.executionAttemptId(), created);
        instructionToAttemptId.put(request.instructionId(), created.executionAttemptId());
        instructionHashToAttemptId.put(replayKey, created.executionAttemptId());
        return PrepareResult.created(created);
    }

    /**
     * The column-ownership guard surface for the attempt-store writer: any
     * column outside the attempt-store group (identity or broker-adapter's)
     * throws, naming the real owner. Called by {@link #prepare} and exercised
     * directly by tests.
     */
    void assertWritableColumns(int... columns) {
        MATRIX.checkWrite(WRITER, columns);
    }

    public AttemptRecord attemptById(String executionAttemptId) {
        return byAttemptId.get(executionAttemptId);
    }

    public int size() {
        return byAttemptId.size();
    }
}
