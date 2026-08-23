package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Max-one-active per instrument — STRICT (user rule 2026-08-18, Option B):
 * if instrument already has an ACTIVE candidate, drop the new one.
 * Do NOT replace — wait until explicit CLOSED.
 *
 * <p>Active state is keyed by {@code instrument_token} and lives
 * INDEFINITELY (no TTL). First signal stores its candidate_id and
 * blocks all later signals for that instrument until an explicit
 * {@code Position_State(status=CLOSED)} arrives on the feedback
 * stream. When that feedback is wired (todo #17) this single-input
 * version becomes a {@code KeyedCoProcessFunction}; until then the
 * block is indefinite and survives restarts (checkpointed).
 * A stuck instrument never auto-frees — it needs a CLOSED or manual
 * admin clear, never a timer.
 *
 * <p>Placed AFTER union of candle + forming-bar signals, BEFORE both
 * LOG and KV sinks — so both twins see the filtered stream.
 */
public class ActiveSignalFilterFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ActiveSignalFilterFunction.class);

    private static final ValueStateDescriptor<String> ACTIVE_ID_DESC =
            new ValueStateDescriptor<>("active-candidate-id", Types.STRING);

    private final SignalJobConfig config;

    private transient ValueState<String> activeIdState;
    private transient Counter droppedCounter;
    private transient Counter passedCounter;

    public ActiveSignalFilterFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        activeIdState = getRuntimeContext().getState(ACTIVE_ID_DESC);
        droppedCounter = getRuntimeContext().getMetricGroup().counter("compute.signal.dropped.active_exists");
        passedCounter = getRuntimeContext().getMetricGroup().counter("compute.signal.passed");
    }

    @Override
    public void processElement(RowData candidate, Context ctx, Collector<RowData> out)
            throws Exception {
        long token = candidate.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN);
        String candidateId = stringAt(candidate, SignalCandidatesTableColumns.CANDIDATE_ID);

        String activeId = activeIdState.value();
        if (activeId != null) {
            // Still active — drop, do not replace. No TTL, no expiry.
            droppedCounter.inc();
            LOG.info("active-signal-filter: dropping {} for instrument {} — active {} (indefinite, wait CLOSED)",
                    candidateId, token, activeId);
            return;
        }

        // Allow — store active indefinitely.
        activeIdState.update(candidateId);
        passedCounter.inc();
        out.collect(candidate);
    }

    /**
     * Admin/test hook: clear active for the current key (instrument).
     * Wired to the Position_State CLOSED feedback in the CoProcess
     * version; exposed here for tests and manual ops.
     */
    void clearActiveForTest() throws Exception {
        activeIdState.clear();
    }

    private static String stringAt(RowData row, int index) {
        return row.isNullAt(index) ? null : row.getString(index).toString();
    }

    // Test accessors
    long droppedForTest() { return droppedCounter == null ? 0 : droppedCounter.getCount(); }
    long passedForTest() { return passedCounter == null ? 0 : passedCounter.getCount(); }
}
