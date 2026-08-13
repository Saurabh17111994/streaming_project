package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Canonical-signal filter (DEC-035, tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001). */
@DisplayName("CanonicalSignalFilterFunction")
class CanonicalSignalFilterFunctionTest {

    private final CanonicalSignalFilterFunction filter = new CanonicalSignalFilterFunction();

    @BeforeEach
    void drainCounter() {
        new ComputeOtlpEmitter("localhost:4318").drainSignalKvFilteredNonCanonicalDelta();
    }

    @AfterEach
    void drainCounterAgain() {
        new ComputeOtlpEmitter("localhost:4318").drainSignalKvFilteredNonCanonicalDelta();
    }

    @Test
    @DisplayName("a row carrying the pinned canonical identity passes through untouched")
    void canonicalRowPasses() {
        assertTrue(filter.filter(canonicalRow()));
    }

    @Test
    @DisplayName("a row with a drifted strategy identity is dropped and counted exactly once")
    void driftedRowIsDroppedAndCounted() {
        GenericRowData row = canonicalRow();
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString("other-strategy"));

        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        assertFalse(filter.filter(row));
        assertFalse(filter.filter(row));
        assertTrue(filter.filter(canonicalRow()));

        // one delta per dropped row, drained once
        assertTrue(emitter.drainSignalKvFilteredNonCanonicalDelta() == 2L);
        assertTrue(emitter.drainSignalKvFilteredNonCanonicalDelta() == 0L);
    }

    @Test
    @DisplayName("a null identity column never passes (strict exact-match)")
    void nullIdentityColumnIsDropped() {
        GenericRowData row = canonicalRow();
        row.setField(SignalCandidatesTableColumns.RULE_ID, null);
        assertFalse(filter.filter(row));
    }

    private static GenericRowData canonicalRow() {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                StringData.fromString("candidate-1"));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, 1000L);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("SYM"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION));
        row.setField(SignalCandidatesTableColumns.RULE_ID,
                StringData.fromString(SignalCandidatesTableColumns.CANONICAL_RULE_ID));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, 1_700_000_000_000L);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, 1_700_000_000_000L);
        row.setField(SignalCandidatesTableColumns.ACTION,
                StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE,
                StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, 1L);
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE,
                StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, null);
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF, null);
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }
}
