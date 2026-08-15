package com.trading.compute.signaljob;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Beyond-allowed-lateness discard observability (REQ-FC-006). The 15 s candle
 * window silently drops raw ticks whose event time is past
 * {@code window_end + ALLOWED_LATENESS_MS}; this turns that silent drop into a
 * counted, attributed metric:
 *
 * <ul>
 *   <li>{@link #OUTPUT} — the {@code OutputTag} the window operator routes
 *       late drops to ({@code sideOutputLateData}), wired in
 *       {@link SignalJob#buildTopology}.</li>
 *   <li>{@link CounterFunction} — the side-output consumer that counts each
 *       drop into {@code compute.candles.late.dropped} (Flink counter) and the
 *       {@link ComputeOtlpEmitter} DELTA mirror, carrying the latest drop's
 *       instrument / window-end / lateness / reason as ONE bounded attribute
 *       set (DEC-038 cardinality rule — never per-key labels).</li>
 * </ul>
 *
 * <p>The counter operator adds no keyed state and emits nothing downstream —
 * observability only. The lateness is measured against the consumer operator's
 * current watermark (side outputs receive the same watermark stream as the
 * main output, so this is the watermark that caused the drop).
 */
public final class CandleLateDrop {

    private CandleLateDrop() {}

    /** Stable drop reason recorded in the metric attributes. */
    public static final String REASON_BEYOND_ALLOWED_LATENESS = "beyond-allowed-lateness";

    /**
     * Side-output tag for raw ticks the candle window drops as late. The
     * dropped rows are the window operator's INPUT layout (the deduped raw
     * tick — {@code RawTableColumns}), not the emitted candle layout.
     */
    public static final OutputTag<RowData> OUTPUT =
            new OutputTag<RowData>("candle-late-dropped") {};

    /**
     * Counting consumer for {@link #OUTPUT}. One DELTA per dropped tick;
     * the latest drop's attributes ride the metric data point.
     */
    public static final class CounterFunction extends ProcessFunction<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private final long candleWindowMs;
        private transient Counter dropped;

        public CounterFunction(long candleWindowMs) {
            this.candleWindowMs = candleWindowMs;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            dropped = getRuntimeContext().getMetricGroup().counter(
                    ComputeOtlpEmitter.CANDLE_LATE_DROPPED_METRIC);
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out) {
            long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
            long windowEnd = (eventTime / candleWindowMs) * candleWindowMs + candleWindowMs;
            long latenessMs = Math.max(0L, ctx.timerService().currentWatermark() - eventTime);
            dropped.inc();
            ComputeOtlpEmitter.recordCandleLateDrop(
                    row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                    windowEnd,
                    latenessMs,
                    REASON_BEYOND_ALLOWED_LATENESS);
        }
    }
}
