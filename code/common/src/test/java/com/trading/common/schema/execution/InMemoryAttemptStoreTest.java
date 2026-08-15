package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SCH-15 first consumer: {@link InMemoryAttemptStore#prepare} runs the
 * column-ownership guard ({@code ColumnOwnership.checkWrite} via the
 * attempt-store matrix), and the store's replay semantics prove the PREPARED
 * attempt's identity columns are never rewritten — duplicate prepare returns
 * the existing record untouched, a modified decision under an existing
 * instruction_id is a contract violation that halts and mutates nothing, and
 * the guard itself rejects identity / foreign-group columns outright.
 */
class InMemoryAttemptStoreTest {

    private static final String ACCOUNT = "acc-1";
    private static final String PARTITION = "p-1";
    private static final long GATE_EPOCH = 7L;
    private static final long NOW_TS = 1_700_000_000_000L;

    private final AtomicInteger halts = new AtomicInteger();
    private InMemoryAttemptStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryAttemptStore(halts::incrementAndGet);
    }

    private static AttemptStore.PrepareRequest req(String attemptId, String instructionId,
                                                   String requestHash) {
        return new AttemptStore.PrepareRequest(attemptId, ACCOUNT, instructionId, null,
                PARTITION, requestHash, "E" + attemptId, GATE_EPOCH, NOW_TS);
    }

    @Test
    void prepareMintsPreparedAttemptWithOwnedDefaults() {
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CREATED);
        AttemptRecord a = r.record();
        assertThat(a.executionAttemptId()).isEqualTo("a-1");
        assertThat(a.instructionId()).isEqualTo("ins-1");
        assertThat(a.requestHash()).isEqualTo("h-1");
        assertThat(a.accountScopeId()).isEqualTo(ACCOUNT);
        assertThat(a.executionPartitionId()).isEqualTo(PARTITION);
        assertThat(a.gateEpoch()).isEqualTo(GATE_EPOCH);
        assertThat(a.phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(a.phaseEpoch()).isZero();
        assertThat(a.retryAttempt()).isZero();
        assertThat(a.preparedTs()).isEqualTo(NOW_TS);
        assertThat(a.schemaVersion()).isEqualTo("2");
        // call-evidence group is null until the broker adapter reports
        assertThat(a.brokerOrderId()).isNull();
        assertThat(a.outcome()).isNull();
        assertThat(a.submittedTs()).isNull();
        assertThat(a.terminalTs()).isNull();
        assertThat(halts.get()).isZero();
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void duplicatePrepareReturnsExistingWithoutMutation() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.DUPLICATE);
        assertThat(r.record().executionAttemptId()).isEqualTo("a-1");
        assertThat(r.record().phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(r.record().phaseEpoch()).isZero();
        // replay returns the SAME record — identity never rewritten, no second row
        assertThat(r.record()).isEqualTo(store.attemptById("a-1"));
        assertThat(store.size()).isEqualTo(1);
        assertThat(halts.get()).isZero();
    }

    @Test
    void conflictingRequestHashRaisesContractViolationAndHalts() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-2"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CONTRACT_VIOLATION);
        assertThat(r.reason()).contains("modified decision");
        // halt requested, nothing created or mutated
        assertThat(halts.get()).isEqualTo(1);
        assertThat(store.size()).isEqualTo(1);
        assertThat(store.attemptById("a-1").requestHash()).isEqualTo("h-1");
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
    }

    @Test
    void prepareMintsUniqueAttemptsForDistinctInstructions() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        store.prepare(req("a-2", "ins-2", "h-2"));
        assertThat(store.size()).isEqualTo(2);
        assertThat(halts.get()).isZero();
    }

    @Test
    void guardRejectsPreparedIdentityColumns() {
        assertThatThrownBy(() -> store.assertWritableColumns(
                ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID,
                ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID,
                ExecutionAttemptsColumns.INSTRUCTION_ID,
                ExecutionAttemptsColumns.ACTION_ID,
                ExecutionAttemptsColumns.EXECUTION_PARTITION_ID,
                ExecutionAttemptsColumns.REQUEST_HASH,
                ExecutionAttemptsColumns.CLIENT_ORDER_REF,
                ExecutionAttemptsColumns.GATE_EPOCH,
                ExecutionAttemptsColumns.SCHEMA_VERSION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void guardRejectsForeignBrokerAdapterGroup() {
        assertThatThrownBy(() -> store.assertWritableColumns(
                ExecutionAttemptsColumns.BROKER_ORDER_ID,
                ExecutionAttemptsColumns.OUTCOME,
                ExecutionAttemptsColumns.OUTCOME_DETAIL,
                ExecutionAttemptsColumns.SUBMITTED_TS,
                ExecutionAttemptsColumns.TERMINAL_TS,
                ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("broker-adapter");
    }

    @Test
    void guardAllowsAttemptStoreOwnedGroup() {
        // the exact mutable group prepare() writes — must always pass
        store.assertWritableColumns(ExecutionAttemptsColumns.PHASE,
                ExecutionAttemptsColumns.PHASE_EPOCH,
                ExecutionAttemptsColumns.PREPARED_TS,
                ExecutionAttemptsColumns.RETRY_ATTEMPT);
        // and prepare() itself runs this assertion on every mint
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CREATED);
    }

    @Test
    void prepareRejectsNullRequest() {
        assertThatThrownBy(() -> store.prepare(null))
                .isInstanceOf(NullPointerException.class);
    }
}
