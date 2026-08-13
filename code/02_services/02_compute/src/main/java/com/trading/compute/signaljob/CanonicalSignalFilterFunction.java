package com.trading.compute.signaljob;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Canonical-signal filter (DEC-035, tracker 14 re-scoped P2 —
 * SIGNAL-SCHEMA-001): stateless pass-through for rows whose
 * {@code (schema_version, strategy_id, strategy_version, rule_id)} identity
 * equals the pinned canonical identity, drop for everything else.
 *
 * <p>Wired between the {@code signals} stream and ONLY the
 * {@code Signal_Candidates_current} KV sink — the LOG twin
 * ({@code Signal_Candidates}) keeps every emitted signal, so no audit history
 * is lost. The filter therefore never reorders, retimes, or rewrites rows: a
 * pass-through row is byte-identical to what the LOG sink writes.
 *
 * <p>Dropped rows are counted in {@code compute.signal.kv.filtered.noncanonical}
 * (DELTA per flush) and WARN-logged with the row's instrument/identity so a
 * misconfigured {@code SIGNAL_STRATEGY_*} override is visible in both the O2
 * stream and the job log. The counters are static mirrors of the single-JVM
 * embedded run (same scope as the other {@code ComputeOtlpEmitter} mirrors).
 *
 * <p>Stateless and deterministic: {@code filter} holds no state and performs
 * no I/O, so replay/restore produces identical KV rows.
 */
public class CanonicalSignalFilterFunction implements FilterFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CanonicalSignalFilterFunction.class);

    @Override
    public boolean filter(RowData row) {
        String schemaVersion = stringAt(row, SignalCandidatesTableColumns.SCHEMA_VERSION);
        String strategyId = stringAt(row, SignalCandidatesTableColumns.STRATEGY_ID);
        String strategyVersion = stringAt(row, SignalCandidatesTableColumns.STRATEGY_VERSION);
        String ruleId = stringAt(row, SignalCandidatesTableColumns.RULE_ID);
        boolean canonical = CanonicalSignalPolicy.isCanonical(
                schemaVersion, strategyId, strategyVersion, ruleId,
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                SignalCandidatesTableColumns.CANONICAL_RULE_ID);
        if (!canonical) {
            ComputeOtlpEmitter.recordSignalKvFilteredNonCanonical();
            LOG.warn("signal-canonical-filter: dropping non-canonical signal from the KV "
                    + "current-state (instrument={}, schema={}, strategy={}:{}, rule={}) — the "
                    + "LOG twin keeps every signal",
                    row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN),
                    schemaVersion, strategyId, strategyVersion, ruleId);
        }
        return canonical;
    }

    private static String stringAt(RowData row, int index) {
        return row.isNullAt(index) ? null : row.getString(index).toString();
    }
}
