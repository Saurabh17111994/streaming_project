package com.trading.compute.signaljob;

import com.trading.compute.telemetry.ComputeAlertLogs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Max-one-active with explicit CLOSED handshake (Option B, strict).
 *
 * <p>Input 1: candidate signals (union of candle + forming-bar).
 * Input 2: Position_State changelog (Nautilus feedback).
 *
 * <p>State is keyed by {@code instrument_token} and is INDEFINITE:
 * first signal stores activeId and blocks all later signals for that
 * instrument until a {@code Position_State.status=CLOSED} row arrives
 * for the same key. CLOSED clears the keyed state — next signal may
 * fire. No TTL, no timer — block survives processing-time advance
 * and restarts (checkpointed). If CLOSED never arrives, the instrument
 * stays blocked forever (correctness over liveness).
 *
 * <p>If Position_State feedback is not yet wired (dev without Nautilus),
 * the function degrades to indefinite single-input block — still correct.
 */
public class ActiveSignalFeedbackFunction
        extends KeyedCoProcessFunction<Long, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ActiveSignalFeedbackFunction.class);

    private static final ValueStateDescriptor<String> ACTIVE_ID_DESC =
            new ValueStateDescriptor<>("active-candidate-id", Types.STRING);

    private final SignalJobConfig config;

    private transient ValueState<String> activeIdState;
    private transient Counter droppedCounter;
    private transient Counter passedCounter;
    private transient Counter clearedCounter;
    // Best-effort per-subtask active count for OpenObserve gauge (inc on pass, dec on clear).
    // Not checkpointed — drifts after restore until next clear/pass re-sync, but sufficient for dashboards.
    // For exact, query Position_State lake where status=OPEN count.
    private transient long activeCount;

    public ActiveSignalFeedbackFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        activeIdState = getRuntimeContext().getState(ACTIVE_ID_DESC);
        droppedCounter = getRuntimeContext().getMetricGroup().counter("compute.signal.dropped.active_exists");
        passedCounter = getRuntimeContext().getMetricGroup().counter("compute.signal.passed");
        clearedCounter = getRuntimeContext().getMetricGroup().counter("compute.signal.cleared.closed");
        activeCount = 0L;
        // OpenObserve gauge: flink_taskmanager_job_task_operator_compute_signal_active_count
        getRuntimeContext().getMetricGroup().gauge("compute.signal.active.count", () -> activeCount);
    }

    @Override
    public void processElement1(RowData candidate, Context ctx, Collector<RowData> out)
            throws Exception {
        long token = candidate.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN);
        String candidateId = stringAt(candidate, SignalCandidatesTableColumns.CANDIDATE_ID);
        String activeId = activeIdState.value();
        if (activeId != null) {
            droppedCounter.inc();
            LOG.info("active-signal-feedback: dropping {} for token {} — active {} (wait CLOSED)",
                    candidateId, token, activeId);
            return;
        }
        activeIdState.update(candidateId);
        passedCounter.inc();
        activeCount++;
        LOG.info("active-signal-feedback: passing {} for token {} — now ACTIVE (activeCount={})", candidateId, token, activeCount);
        out.collect(candidate);
    }

    @Override
    public void processElement2(RowData positionState, Context ctx, Collector<RowData> out)
            throws Exception {
        long token = positionState.getLong(PositionStateTableColumns.INSTRUMENT_TOKEN);
        String status = stringAt(positionState, PositionStateTableColumns.STATUS);
        String activeId = activeIdState.value();
        if (PositionStateTableColumns.STATUS_CLOSED.equals(status)
                || PositionStateTableColumns.STATUS_ADMIN_CLEAR.equals(status)) {
            if (activeId != null) {
                LOG.info("active-signal-feedback: {} for token {} — clearing active {} (was {})",
                        status, token, activeId, status);
                activeIdState.clear();
                clearedCounter.inc();
                if (activeCount > 0) activeCount--;
                // Structured event for OpenObserve trading_alerts (queryable token/status)
                ComputeAlertLogs.emitAlertLog(config.otelCollectorHost(), "INFO",
                        "active-signal-cleared",
                        "token=" + token + " status=" + status + " clearedActive=" + activeId + " activeCount=" + activeCount);
            } else {
                LOG.info("active-signal-feedback: {} for token {} — no active to clear (idempotent)", status, token);
            }
            if (PositionStateTableColumns.STATUS_ADMIN_CLEAR.equals(status)) {
                // ADMIN_CLEAR is an ops break-glass — same as CLOSED but distinct audit.
                getRuntimeContext().getMetricGroup().counter("compute.signal.cleared.admin").inc();
                if (activeId != null) {
                    ComputeAlertLogs.emitAlertLog(config.otelCollectorHost(), "WARN",
                            "active-signal-admin-clear",
                            "token=" + token + " clearedActive=" + activeId + " activeCount=" + activeCount);
                }
            }
        } else if (PositionStateTableColumns.STATUS_OPEN.equals(status)) {
            // OPEN is written by execution layer when it opens position.
            // We don't set active here — Flink already did on signal.
            // Log only for observability.
            LOG.debug("active-signal-feedback: OPEN for token {} — active {}", token, activeId);
        } else {
            LOG.warn("active-signal-feedback: unknown status '{}' for token {} — active {} (ignored)", status, token, activeId);
        }
    }

    private static String stringAt(RowData row, int index) {
        return row.isNullAt(index) ? null : row.getString(index).toString();
    }
}
