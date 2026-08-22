package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * Emits exactly one {@code feature_candles_15s} row per non-empty 15-second
 * window (Signal dossier operator 3, R-012: no correction rows in MVP).
 *
 * <p>Window semantics: the window fires at {@code window_end}; a late event
 * arriving within {@code ALLOWED_LATENESS_MS} folds into the accumulator and
 * re-triggers this function. The {@code emitted} window-state flag makes the
 * re-trigger a no-op except for a {@code compute.candles.late.updates} count —
 * the already-written candle is never corrected or duplicated. Events arriving
 * beyond the lateness bound are dropped by the window operator (standard Flink
 * late-drop) to the {@link CandleLateDrop} side output, where
 * {@code compute.candles.late.dropped} counts each drop (REQ-FC-006). The KV
 * write path adds a second, restore-surviving first-write-wins layer:
 * {@link CandleKvFirstWriteWinsFunction} (keyed by the candle PK
 * {@code (instrument_token, window_start)}) drops and counts a second window
 * emission via {@code compute.candles.duplicate_window} — the emission flag
 * here guards a window's lifetime, the KV guard shields the already-written
 * row across checkpoint-restore re-fires (streaming-3000 T5, decision 25).
 *
 * <p>Streaming-3000 T6 (decision 24): before the candle is handed off, the
 * five {@link CandleInvariantCheck} OHLC invariants run. A violation routes
 * the candle to the {@link CandleQuarantine#OUTPUT} side output (one
 * evidence row in {@code ingestion_quarantine}, counted into
 * {@code compute.candles.invalid.*}) and it is NOT emitted to the main
 * output — so it never reaches {@code feature_candles_15s}, the KV
 * first-write-wins guard, or signal detection. The {@code emitted}
 * window-state flag is still set for an invalid window, so a late re-trigger
 * cannot double-quarantine the same window.
 *
 * <p>Version columns are the pinned algorithm/configuration/schema versions
 * (REQ-FC-001: {@code algorithm_version}/{@code configuration_version} replace
 * the single {@code candle_version}); {@code output_ts} is the processing-time
 * emit instant.
 */
public class CandleEmitFunction extends ProcessWindowFunction<CandleAccumulator, RowData, Long, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<Boolean> EMITTED_DESCRIPTOR =
            new ValueStateDescriptor<>("candle-emitted", Types.BOOLEAN);

    private final SignalJobConfig config;

    private transient Counter emittedCounter;
    private transient Counter lateUpdateCounter;

    public CandleEmitFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        emittedCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.emitted");
        lateUpdateCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.late.updates");
    }

    @Override
    public void process(Long instrumentToken, Context context, Iterable<CandleAccumulator> elements,
            Collector<RowData> out) throws Exception {
        CandleAccumulator acc = elements.iterator().next();

        ValueState<Boolean> emitted = context.windowState().getState(EMITTED_DESCRIPTOR);
        if (Boolean.TRUE.equals(emitted.value())) {
            lateUpdateCounter.inc();
            return;
        }
        TimeWindow window = context.window();
        emitted.update(true);
        // Streaming-3000 T6: five OHLC invariants gate the emit. A violating
        // candle is quarantined (side output + counter) and never emitted —
        // the main output only ever carries invariant-clean candles.
        CandleInvariantCheck.Reason violation =
                CandleInvariantCheck.firstViolation(acc, window, config.candleWindowMs());
        if (violation != null) {
            context.output(CandleQuarantine.OUTPUT,
                    CandleQuarantine.buildRow(instrumentToken, acc, window, violation,
                            "compute-candle-" + UUID.randomUUID(),
                            context.currentProcessingTime()));
            return;
        }
        emittedCounter.inc();
        out.collect(buildRow(instrumentToken, acc, window, context.currentProcessingTime(), config));
    }

    /**
     * Builds the 15-column candle row (CandleTableColumns order) for the
     * given window. Extracted so the version-carrying contract is testable
     * without a window-operator harness (Flink 2.x removed
     * ProcessWindowFunctionTestHarness).
     */
    static GenericRowData buildRow(long instrumentToken, CandleAccumulator acc, TimeWindow window,
            long outputTs, SignalJobConfig config) {
        GenericRowData row = new GenericRowData(CandleTableColumns.FIELD_COUNT);
        row.setField(CandleTableColumns.INSTRUMENT_TOKEN, instrumentToken);
        row.setField(CandleTableColumns.EXCHANGE, StringData.fromString(acc.exchange));
        row.setField(CandleTableColumns.SYMBOL, StringData.fromString(acc.symbol));
        row.setField(CandleTableColumns.WINDOW_START, window.getStart());
        row.setField(CandleTableColumns.WINDOW_END, window.getEnd());
        row.setField(CandleTableColumns.OPEN_PAISE, acc.openPaise);
        row.setField(CandleTableColumns.HIGH_PAISE, acc.highPaise);
        row.setField(CandleTableColumns.LOW_PAISE, acc.lowPaise);
        row.setField(CandleTableColumns.CLOSE_PAISE, acc.closePaise);
        row.setField(CandleTableColumns.VOLUME, acc.volume);
        row.setField(CandleTableColumns.TICK_COUNT, (int) acc.tickCount);
        row.setField(CandleTableColumns.ALGORITHM_VERSION, StringData.fromString(config.algorithmVersion()));
        row.setField(CandleTableColumns.CONFIGURATION_VERSION, StringData.fromString(config.configurationVersion()));
        row.setField(CandleTableColumns.OUTPUT_TS, outputTs);
        row.setField(CandleTableColumns.SCHEMA_VERSION, StringData.fromString(config.candleSchemaVersion()));
        return row;
    }
}
