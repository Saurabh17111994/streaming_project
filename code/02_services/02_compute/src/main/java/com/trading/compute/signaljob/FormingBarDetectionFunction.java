package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * Forming-bar placeholder detector (Slice 2.2 forming-bar handoff): the
 * Business Logic consumer that receives live {@link FormingBar} events and
 * emits {@code Signal_Candidates} rows via the shared candidate contract.
 *
 * <p><b>Two inputs, same instrument key.</b> {@code processElement1} receives
 * the live forming-bar event (one per accepted tick — REQ-FC-007);
 * {@code processElement2} receives completed candles from the existing candle
 * pipeline (the same stream that feeds {@code SignalDetectionFunction}) and
 * maintains the lookback ring buffers. Both streams are keyed by
 * {@code instrument_token} and connected with matching key selectors, so a
 * forming-bar event for instrument X only ever sees completed-candle history
 * for instrument X (Flink keyed-state scoping).
 *
 * <p><b>Placeholder rule — "mirrored breakout" (PLACEHOLDER, not the real
 * strategy).</b> Mirrors the Slice 2.1 rule shape on the live forming bar:
 * <ol>
 *   <li><b>Bullish</b>: {@code close > open} (strict).</li>
 *   <li><b>Breakout</b>: {@code close > max(high of the previous
 *       {@code FORMING_LOOKBACK_CANDLES} completed candles)} (strict).</li>
 *   <li><b>Trend filter</b>: {@code close > mean(close of the previous
 *       completed candles)} — exact integer compare {@code close * n > sum}.</li>
 * </ol>
 * No evaluation before {@code lookback} completed candles exist per instrument
 * (warm-up). Lookback history is strictly prior: only candles with
 * {@code windowEnd <= formingBar.windowStart()} enter the buffers, so the
 * forming window's own candle (not yet closed) and future candles can never
 * leak into the comparison.
 *
 * <p><b>Fire-once per forming window — PLACEHOLDER-ONLY semantics.</b> The
 * detector fires at most one candidate per (instrument, forming window):
 * {@code forming-fired} {@code ValueState} records the fired window start and
 * is reset only when a new window's event arrives. This exists for
 * deterministic integration testing of THIS placeholder; it is NOT a semantic
 * of the forming-bar event contract or the handoff — future real strategies
 * remain free to react per update, revise, fire once, or fire many times. The
 * {@link FormingBar} event itself carries no fire-once encoding.
 *
 * <p>Fire-once survives checkpoint recovery (keyed {@code ValueState}, the
 * same mechanism as {@code CandleEmitFunction}'s emitted flag) — replayed
 * events for an already-fired window emit nothing, so duplicate updates can
 * never generate duplicate candidates.
 *
 * <p><b>Candidate identity:</b> {@code rule_id-instrument_token-window_start}
 * — unique because the placeholder fires once per (instrument, window).
 * Downstream: the candidate rows union into the existing signal dual-sink
 * ({@code Signal_Candidates} LOG + {@code Signal_Candidates_current} KV via
 * the canonical filter, which now admits {@code CANONICAL_FORMING_RULE_ID}).
 * Candidates are observational/test data in the current build — the only
 * executable feed ({@code Trade_Decisions}) has no producer and the Executor
 * consumes {@code Trade_Decisions} only (see the dossier).
 */
public class FormingBarDetectionFunction
        extends KeyedCoProcessFunction<Long, FormingBar, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<List<Long>> HIGHS_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-candle-highs", Types.LIST(Types.LONG));
    private static final ValueStateDescriptor<List<Long>> CLOSES_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-candle-closes", Types.LIST(Types.LONG));
    private static final ValueStateDescriptor<Long> FIRED_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-fired-window-start", Types.LONG);
    private static final ValueStateDescriptor<Long> CURRENT_WINDOW_DESCRIPTOR =
            new ValueStateDescriptor<>("forming-current-window-start", Types.LONG);

    private final SignalJobConfig config;

    private transient ValueState<List<Long>> highsState;
    private transient ValueState<List<Long>> closesState;
    private transient ValueState<Long> firedState;
    private transient ValueState<Long> currentWindowState;
    private transient Counter detectedCounter;

    public FormingBarDetectionFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        highsState = getRuntimeContext().getState(HIGHS_DESCRIPTOR);
        closesState = getRuntimeContext().getState(CLOSES_DESCRIPTOR);
        firedState = getRuntimeContext().getState(FIRED_DESCRIPTOR);
        currentWindowState = getRuntimeContext().getState(CURRENT_WINDOW_DESCRIPTOR);
        detectedCounter = getRuntimeContext().getMetricGroup().counter("compute.signals.detected.forming");
    }

    /** Live forming-bar event (input 1): evaluate the placeholder rule. */
    @Override
    public void processElement1(FormingBar bar, Context ctx, Collector<RowData> out)
            throws Exception {
        // Track the current forming window (drives the strictly-prior guard on
        // completed-candle ingestion below). Updated BEFORE evaluation so this
        // event's own window is the reference for the history it compares
        // against; the fire-once flag resets naturally when a later event
        // carries a different windowStart.
        currentWindowState.update(bar.windowStart());

        List<Long> highs = highsState.value();
        List<Long> closes = closesState.value();
        int lookback = config.formingLookbackCandles();

        boolean warm = highs != null && closes != null
                && highs.size() >= lookback && closes.size() >= lookback;
        if (!warm) {
            return; // warm-up: not enough completed-candle history yet
        }

        Long firedWindow = firedState.value();
        if (firedWindow != null && firedWindow == bar.windowStart()) {
            return; // fire-once per forming window (placeholder-only semantics)
        }

        long maxHigh = 0;
        for (long h : highs) {
            maxHigh = Math.max(maxHigh, h);
        }
        long sumCloses = 0;
        for (long c : closes) {
            sumCloses += c;
        }
        boolean bullish = bar.closePaise() > bar.openPaise();
        boolean breakout = bar.closePaise() > maxHigh;
        boolean trend = bar.closePaise() * (long) closes.size() > sumCloses;
        if (!(bullish && breakout && trend)) {
            return;
        }

        firedState.update(bar.windowStart());
        detectedCounter.inc();
        out.collect(toCandidate(bar));
    }

    /** Completed candle (input 2): maintain the lookback ring buffers. */
    @Override
    public void processElement2(RowData candle, Context ctx, Collector<RowData> out)
            throws Exception {
        long candleWindowEnd = candle.getLong(CandleTableColumns.WINDOW_END);
        // Strictly-prior guard: a completed candle enters the lookback only if
        // its window END is <= the current forming window START. The completed
        // candle whose window ends exactly at the forming window's start IS the
        // immediately-preceding closed candle and legitimately belongs in the
        // history; the forming window's own candle (windowEnd == forming
        // windowEnd > start) and any future candle never do. This closes the
        // mid-window arrival edge: the candle for [W-15, W) can be emitted
        // WHILE window [W, W+15) is still forming (watermark crosses W mid-
        // window), and the guard admits it (windowEnd == W <= W) while
        // rejecting anything from window [W, W+15) or later.
        Long currentWindow = currentWindowState.value();
        if (currentWindow != null && candleWindowEnd > currentWindow) {
            return;
        }
        addToBuffers(candle);
    }

    private void addToBuffers(RowData candle) throws Exception {
        long high = candle.getLong(CandleTableColumns.HIGH_PAISE);
        long close = candle.getLong(CandleTableColumns.CLOSE_PAISE);

        List<Long> highs = highsState.value();
        if (highs == null) {
            highs = new ArrayList<>();
        }
        List<Long> closes = closesState.value();
        if (closes == null) {
            closes = new ArrayList<>();
        }

        int lookback = config.formingLookbackCandles();
        highs.add(high);
        closes.add(close);
        while (highs.size() > lookback) {
            highs.remove(0);
        }
        while (closes.size() > lookback) {
            closes.remove(0);
        }
        highsState.update(highs);
        closesState.update(closes);
    }

    private RowData toCandidate(FormingBar bar) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        String candidateId = config.formingRuleId() + "-" + bar.instrumentToken()
                + "-" + bar.windowStart();
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, bar.instrumentToken());
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString(bar.exchange()));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString(bar.symbol()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString(config.signalStrategyId()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString(config.signalStrategyVersion()));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(config.formingRuleId()));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, bar.lastEventTime());
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, bar.lastEventTime());
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, config.signalQuantity());
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE, StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, null);
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF,
                StringData.fromString("forming-bar:" + bar.windowStart() + ":" + bar.windowEnd()
                        + ":open=" + bar.openPaise() + ":high=" + bar.highPaise()
                        + ":low=" + bar.lowPaise() + ":close=" + bar.closePaise()
                        + ":volume=" + bar.volume()));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }
}
