package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Forming-bar builder (Slice 2.2 forming-bar handoff, REQ-FC-007): consumes
 * the deduped, validated tick stream keyed by {@code instrument_token} and
 * maintains the CURRENT 15-second forming bar per instrument, emitting a
 * {@link FormingBar} record on EVERY accepted tick — Business Logic sees the
 * latest forming state immediately, with no wait for the window to close and
 * no Fluss round trip.
 *
 * <p><b>Live forming state, not history:</b> this operator holds only the
 * current window's accumulator (bounded by instrument count — DEC-038 open-
 * candle working state). Per-tick snapshots are transient stream events; they
 * are never retained in Flink state and never written anywhere. The FINALIZED
 * candle is produced exclusively by the completed-candle pipeline (the window
 * operator) — this operator finalizes nothing and never emits a candle.
 *
 * <p><b>Aggregation semantics:</b> identical to the candle path — this
 * operator reuses {@link CandleAccumulator} + {@link CandleAggregateFunction}
 * (REQ-FC-002: OHLC from {@code last_price_paise} on trades AND quotes,
 * volume/tick_count only on {@code TRADE} rows with {@code last_qty > 0},
 * open/close by the deterministic {@code (event_time, event_fingerprint)}
 * order key). Window alignment is epoch-aligned
 * ({@code floor(event_time / CANDLE_WINDOW_MS) * CANDLE_WINDOW_MS}) — the
 * same alignment the tumbling window operator uses, so the forming bar and
 * the eventual finalized candle always agree.
 *
 * <p>Per-tick event volume is intentional (REQ-FC-007 "whenever an eligible
 * trade updates the current forming bar"); the counter
 * {@code compute.forming.bar.updates} measures the update rate (REQ-FC-010).
 */
public class FormingBarBuilderFunction extends KeyedProcessFunction<Long, RowData, FormingBar> {

    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<CandleAccumulator> ACC_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-bar-acc", CandleAccumulator.class);
    private static final ValueStateDescriptor<Long> WINDOW_START_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-bar-window-start", Long.class);

    private final SignalJobConfig config;
    private final CandleAggregateFunction aggregate;

    private transient ValueState<CandleAccumulator> accState;
    private transient ValueState<Long> windowStartState;
    private transient Counter updateCounter;

    public FormingBarBuilderFunction(SignalJobConfig config) {
        this.config = config;
        this.aggregate = new CandleAggregateFunction();
    }

    @Override
    public void open(OpenContext openContext) {
        accState = getRuntimeContext().getState(ACC_DESCRIPTOR);
        windowStartState = getRuntimeContext().getState(WINDOW_START_DESCRIPTOR);
        updateCounter = getRuntimeContext().getMetricGroup().counter("compute.forming.bar.updates");
    }

    @Override
    public void processElement(RowData tick, Context ctx, Collector<FormingBar> out)
            throws Exception {
        long eventTime = tick.getLong(RawTableColumns.EVENT_TIME);
        long windowMs = config.candleWindowMs();
        // Epoch-aligned window start — the same alignment as TumblingEventTimeWindows.
        long windowStart = (eventTime / windowMs) * windowMs;

        Long storedWindow = windowStartState.value();
        CandleAccumulator acc = accState.value();
        if (storedWindow == null || storedWindow != windowStart || acc == null) {
            // First tick of a new window: start a fresh bar (open = this tick's price).
            acc = new CandleAccumulator();
            windowStartState.update(windowStart);
        }

        // Same accumulation semantics as the candle path (REQ-FC-002).
        aggregate.add(tick, acc);
        accState.update(acc);

        updateCounter.inc();

        out.collect(new FormingBar(
                tick.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                windowStart,
                windowStart + windowMs,
                acc.openPaise,
                acc.highPaise,
                acc.lowPaise,
                acc.closePaise,
                acc.volume,
                acc.tickCount,
                acc.lastEventTime == Long.MIN_VALUE ? eventTime : acc.lastEventTime,
                acc.lastFingerprint,
                acc.exchange,
                acc.symbol));
    }
}
