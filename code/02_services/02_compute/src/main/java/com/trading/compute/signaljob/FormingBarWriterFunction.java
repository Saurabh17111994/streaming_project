package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Durable-write cadence for the {@code forming_bar} KV current-state
 * projection (forming-bar persistence phase, 2026-08-16). Keyed by
 * {@code instrument_token}, this operator coalesces the per-tick
 * {@link FormingBar} stream from the builder's {@code PERSIST_OUTPUT} side
 * output to <b>one row per instrument</b> — the latest forming bar, replacing
 * any previous state (current-state semantics: PK {@code instrument_token},
 * last-write-wins, never append-only history, never per-tick snapshots) — and
 * emits it on a processing-time cadence ({@code FORMING_BAR_WRITE_BATCH_MS})
 * for the downstream {@code FlussSink} (INSERT → UPSERT). The sink aligns to
 * checkpoint barriers, so the worst durable-write window is bounded by the
 * write cadence plus the sink's own batching — no per-tick Fluss write is
 * ever placed on the forming-bar hot path.
 *
 * <p><b>State ownership (DEC-038):</b> Fluss owns the durable forming-bar
 * state; this operator holds only the bounded active context — one buffered
 * row per instrument (bounded by instrument count, not by update rate). The
 * buffer is cleared after each flush: a quiet instrument writes nothing until
 * its forming bar moves again, and a restart rehydrates the row from Fluss
 * (the durable authority), not from this operator's state.
 */
public class FormingBarWriterFunction extends KeyedProcessFunction<Long, FormingBar, RowData> {

    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<FormingBar> LATEST_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-bar-latest", FormingBarTypeInfo.INSTANCE);
    private static final ValueStateDescriptor<Long> PENDING_TIMER_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-bar-pending-timer", Long.class);

    private final SignalJobConfig config;

    private transient ValueState<FormingBar> latest;
    private transient ValueState<Long> pendingTimer;

    public FormingBarWriterFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        latest = getRuntimeContext().getState(LATEST_DESCRIPTOR);
        pendingTimer = getRuntimeContext().getState(PENDING_TIMER_DESCRIPTOR);
    }

    @Override
    public void processElement(FormingBar bar, Context ctx, Collector<RowData> out)
            throws Exception {
        // Current-state coalescing: the latest snapshot for this instrument
        // REPLACES the buffered one — the table never holds per-tick history.
        latest.update(bar);
        if (pendingTimer.value() == null) {
            long fireAt = ctx.timerService().currentProcessingTime()
                    + config.formingBarWriteBatchMs();
            ctx.timerService().registerProcessingTimeTimer(fireAt);
            pendingTimer.update(fireAt);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        pendingTimer.clear();
        FormingBar bar = latest.value();
        if (bar == null) {
            return;
        }
        out.collect(FormingBarRowMapper.toRow(bar));
        // The row is now handed to the sink (durable at the next barrier);
        // Fluss is the authority. Clear the bounded active context so a quiet
        // instrument holds nothing (DEC-038: small working state).
        latest.clear();
    }
}
