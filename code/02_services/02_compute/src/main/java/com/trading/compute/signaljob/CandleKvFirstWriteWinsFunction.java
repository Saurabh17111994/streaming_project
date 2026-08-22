package com.trading.compute.signaljob;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * KV-level first-write-wins guard for the {@code feature_candles_15s} sink
 * (streaming-3000 hardening plan decision 25 — "Dup window: first-write-wins,
 * skip second"; T5). Sits between the candle window operator and the Fluss KV
 * sink; keyed by the candle primary key {@code (instrument_token,
 * window_start)}.
 *
 * <p><b>Why this layer exists.</b> {@link CandleEmitFunction}'s {@code emitted}
 * window-state flag already guarantees exactly one emission per window within
 * the window operator's lifetime, but it cannot see a second emission that
 * survives a checkpoint-restore cycle (a window re-fires when the restore
 * predates its fire; the emit flag rolls back with the window state) or a
 * manual same-day savepoint rollback. This operator makes the write path
 * self-protecting: the KV row for a given {@code (token, window_start)} is
 * written at most once per process lifetime. A second emission of the same
 * key is <b>not re-written</b> — it is dropped and counted into the
 * {@code compute.candles.duplicate_window} MetricGroup counter (the plan's
 * duplicate-window telemetry, previously deferred per CANDLE-KV-REPLAY-001
 * observability note; now implemented by streaming-3000 T5).
 *
 * <p><b>Semantics.</b>
 * <ul>
 *   <li>First emission for a key → mark in keyed state, forward the row
 *       unchanged to the KV sink (the sink's INSERT→UPSERT conversion then
 *       writes it; on full replay with fresh state every replayed candle is a
 *       first write and the upserts converge — the replay contract is
 *       unchanged).</li>
 *   <li>Second emission for the same key within the state TTL → drop,
 *       {@code compute.candles.duplicate_window}++ (a re-emission cannot
 *       correct an already-written candle — no correction rows, R-012).</li>
 *   <li>Empty windows produce no elements, so this operator emits nothing for
 *       them (empty-window no-row invariant preserved).</li>
 * </ul>
 *
 * <p><b>TTL and boundedness.</b> The written-marker is {@code ValueState<Boolean>}
 * with native {@code StateTtlConfig} TTL {@value #WRITTEN_MARK_TTL_HOURS} h,
 * {@code OnCreateAndWrite} (anchor = first write; the marker is never
 * rewritten) and {@code NeverReturnExpired} (an expired marker reads as
 * absent — a re-arrival after TTL is re-admitted as a fresh first write,
 * which is benign: replay is deterministic via the
 * {@code (event_time, fingerprint)} order keys, so the re-written row content
 * is identical to the existing KV row). A legitimate second emission can only
 * arrive within the window's lateness horizon (≤ window + allowed lateness,
 * already suppressed by the emit flag) or from a checkpoint/savepoint restore
 * (checkpoint cadence 10 s; a same-day rollback is the governed operational
 * horizon) — one trading day of marker state (≈4.7 M keys at the 3k design
 * envelope) is a large over-coverage with a hard upper bound.
 *
 * <p>State contract (SIG-UNIT-008 style): per emitted key exactly ONE Boolean
 * value — no candle payload, no row, no collection. No timers (native TTL
 * expiry); no Fluss RPC on the write path (CHG-022 rule).
 */
public class CandleKvFirstWriteWinsFunction
        extends KeyedProcessFunction<Tuple2<Long, Long>, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** ValueState key: the written marker. */
    private static final String WRITTEN_STATE_NAME = "candle-kv-written";

    private static final long WRITTEN_MARK_TTL_HOURS = 24L;

    /**
     * Written-marker retention: one trading day. Covers the operational
     * second-emission horizons (window lateness re-trigger, restore from a
     * recent checkpoint, same-day savepoint rollback) with wide margin while
     * bounding marker state to roughly one day of candle keys. Package-visible
     * so the TTL-expiry test can advance the harness TTL clock past it.
     */
    static final Duration WRITTEN_MARK_TTL = Duration.ofHours(WRITTEN_MARK_TTL_HOURS);

    private transient ValueState<Boolean> written;
    private transient Counter duplicateWindows;

    /** The candle-stream key selector: primary key of feature_candles_15s. */
    public static KeySelector<RowData, Tuple2<Long, Long>> keySelector() {
        return row -> Tuple2.of(
                row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                row.getLong(CandleTableColumns.WINDOW_START));
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>(WRITTEN_STATE_NAME, Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(WRITTEN_MARK_TTL)
                // Expired marker reads as absent — the re-arrival is re-admitted
                // (deterministic replay makes the rewrite content-identical).
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // Anchor at first write; the marker is never rewritten.
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build());
        written = getRuntimeContext().getState(descriptor);

        // Streaming-3000 decision 25 (T5): every second window emission for an
        // already-written (token, window_start) is counted here — the KV
        // duplicate counter previously deferred under CANDLE-KV-REPLAY-001.
        duplicateWindows = getRuntimeContext().getMetricGroup()
                .counter("compute.candles.duplicate_window");
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out)
            throws Exception {
        if (written.value() != null) {
            // Second window emission for this (token, window_start): the KV row
            // is final from its first write — drop, never re-write (first-write
            // wins, no correction rows).
            duplicateWindows.inc();
            return;
        }
        written.update(true);
        out.collect(row);
    }

    /** Counter-source accessor (tests): the value the MetricGroup counter exports. */
    long duplicateWindowCountForTest() {
        return duplicateWindows == null ? 0L : duplicateWindows.getCount();
    }
}
