package com.trading.compute.babysitter;

import com.trading.common.schema.KvStateUpdateProtocol;
import com.trading.common.schema.position.PositionSnapshot;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed (by {@code position_id}) observation operator (Task 7).
 *
 * <p>Applies {@link KvStateUpdateProtocol} semantics to the versioned
 * {@code Positions} changelog: same version/same source event is a no-op
 * (DUPLICATE); a lower version is STALE; an equal version with different
 * source event is CONFLICT (or REGRESSION for a backward version); any
 * ambiguous/unknown disposition is counted as a conflict and never applied.
 * The operator stores only the latest accepted version/freshness metadata in
 * checkpointed {@link ValueState} and <b>never emits a row</b> to its
 * collector — the terminal sink is a deliberate no-op, so no
 * {@code Position_Actions}, lifecycle, position, or execution record can be
 * produced, and there is no broker/Arrow call surface (the Babysitter cannot
 * issue broker commands).
 */
public final class PositionsObservationOperator
        extends KeyedProcessFunction<String, PositionSnapshot, Void> {

    private static final long serialVersionUID = 1L;

    /**
     * Observability side-output carrying each row's {@link KvStateUpdateProtocol.Outcome}.
     * Routed to no sink in production; read by tests/health so stale, conflict,
     * and duplicate streams are observable while the MAIN output stays empty
     * (zero {@code Position_Actions}).
     */
    public static final OutputTag<KvStateUpdateProtocol.Outcome> DISPOSITION =
            new OutputTag<KvStateUpdateProtocol.Outcome>("babysitter-observation-disposition") {};

    private transient ValueState<PositionsObservationState> state;
    private transient Counter observed;
    private transient Counter applied;
    private transient Counter duplicate;
    private transient Counter stale;
    private transient Counter conflict;

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<PositionsObservationState> desc =
                new ValueStateDescriptor<>(
                        "babysitter-position-observation",
                        TypeInformation.of(PositionsObservationState.class));
        state = getRuntimeContext().getState(desc);

        observed = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.observed");
        applied = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.applied");
        duplicate = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.duplicate");
        stale = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.stale");
        conflict = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.conflict");
        getRuntimeContext().getMetricGroup().gauge(
                "babysitter.positions.latest_observed_version",
                (Gauge<Long>) this::peekSourceVersion);
    }

    @Override
    public void processElement(PositionSnapshot snap, Context ctx, Collector<Void> out)
            throws Exception {
        observed.inc();
        PositionsObservationState cur = state.value();
        // The protocol treats a negative version as UNKNOWN (invalid input), so
        // a fresh key (no prior state) is applied directly rather than fed a
        // sentinel -1 through evaluate().
        KvStateUpdateProtocol.Outcome outcome;
        if (cur == null) {
            outcome = KvStateUpdateProtocol.Outcome.APPLIED;
        } else {
            boolean contentMatches =
                    Objects.equals(cur.getSourceEventId(), snap.sourceEventId());
            outcome = KvStateUpdateProtocol.evaluate(
                    cur.getSourceVersion(), snap.sourceVersion(), contentMatches);
        }

        switch (outcome) {
            case APPLIED -> {
                state.update(new PositionsObservationState(
                        snap.sourceVersion(),
                        snap.sourceEventId(),
                        snap.lastUpdateTs(),
                        snap.schemaVersion()));
                applied.inc();
            }
            case DUPLICATE -> duplicate.inc();
            case STALE -> stale.inc();
            // CONFLICT, REGRESSION, UNKNOWN — any ambiguous/stale-backward
            // disposition is a conflict: never applied, never an action.
            default -> conflict.inc();
        }

        // Intentionally never call out.collect(...): the Babysitter observes
        // only. No Position_Actions, no persistence, no broker command can be
        // issued from this operator. The disposition is observable on the side
        // channel for health, never on the main stream.
        ctx.output(DISPOSITION, outcome);
    }

    /** Latest accepted source version across the keyed subtask (metrics). */
    private long peekSourceVersion() {
        try {
            PositionsObservationState cur = state == null ? null : state.value();
            return cur == null ? -1L : cur.getSourceVersion();
        } catch (Exception e) {
            return -1L;
        }
    }

}
