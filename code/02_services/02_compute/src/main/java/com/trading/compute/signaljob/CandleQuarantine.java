package com.trading.compute.signaljob;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compute-side candle quarantine (streaming-3000 T6, decision 24). A candle
 * that fails one of the five {@link CandleInvariantCheck} OHLC invariants is
 * routed off the emit path by {@link CandleEmitFunction} onto {@link #OUTPUT}
 * — a side output of the {@code candle-15s} window operator — instead of the
 * main output: it is never written to {@code feature_candles_15s} and never
 * reaches signal detection. Each quarantined candle becomes one evidence row
 * in the {@code ingestion_quarantine} LOG table ({@link
 * CandleQuarantineColumns}, DDL 21 — the same table the ingestion pipeline
 * uses for invalid raw ticks), counted into
 * {@code compute.candles.invalid.total} + the per-reason
 * {@code compute.candles.invalid.<reason>} MetricGroup counters by
 * {@link CounterFunction}.
 *
 * <p>Evidence contract (mirrors the ingestion QuarantineWriter): the row
 * carries {@code raw_payload} = the offending candle rendered as a compact
 * JSON document (the only evidence compute has — there is no broker packet on
 * this path), {@code payload_hash} = its SHA-256 hex, and a bounded
 * {@code detail} column for operators. {@code quarantine_id} is a fresh UUID
 * per row: the LOG is append-only, and a restore re-fire of an invalid window
 * legitimately re-quarantines (one evidence row per violation event).
 */
public final class CandleQuarantine {

    private CandleQuarantine() {}

    private static final Logger LOG = LoggerFactory.getLogger(CandleQuarantine.class);

    /** Detail length bound — matches the ingestion writer's scrubbed-detail cap. */
    private static final int DETAIL_MAX_LENGTH = 512;

    /** Side-output tag: invalid candles the window operator quarantines. */
    public static final OutputTag<RowData> OUTPUT =
            new OutputTag<RowData>("candle-invalid-quarantine") {};

    /**
     * Builds the 10-column quarantine row ({@link CandleQuarantineColumns}
     * order, DDL 21) for a candle that failed {@code reason}.
     *
     * @param acc          the offending window accumulator
     * @param window       the window the candle describes
     * @param reason       the first failing invariant check (deterministic)
     * @param quarantineId row-unique id (fresh UUID per violation event)
     * @param detectedTsMs emit instant (processing-time epoch ms)
     */
    /**
     * Builds the 10-column quarantine row ({@link CandleQuarantineColumns}
     * order, DDL 21) for a candle that failed {@code reason}.
     *
     * @param instrumentToken the window key (the candle's instrument)
     * @param acc             the offending window accumulator
     * @param window          the window the candle describes
     * @param reason          the first failing invariant check (deterministic)
     * @param quarantineId    row-unique id (fresh UUID per violation event)
     * @param detectedTsMs    emit instant (processing-time epoch ms)
     */
    static GenericRowData buildRow(long instrumentToken, CandleAccumulator acc, TimeWindow window,
            CandleInvariantCheck.Reason reason, String quarantineId, long detectedTsMs) {
        byte[] evidence = evidence(instrumentToken, acc, window, reason);
        GenericRowData row = new GenericRowData(CandleQuarantineColumns.FIELD_COUNT);
        row.setField(CandleQuarantineColumns.QUARANTINE_ID,
                StringData.fromString(quarantineId));
        row.setField(CandleQuarantineColumns.REASON,
                StringData.fromString(reason.quarantineCode()));
        row.setField(CandleQuarantineColumns.INSTRUMENT_TOKEN, instrumentToken);
        row.setField(CandleQuarantineColumns.EXCHANGE, StringData.fromString(acc.exchange));
        row.setField(CandleQuarantineColumns.SYMBOL, StringData.fromString(acc.symbol));
        row.setField(CandleQuarantineColumns.RAW_PAYLOAD, evidence);
        row.setField(CandleQuarantineColumns.PAYLOAD_HASH,
                StringData.fromString(sha256Hex(evidence)));
        row.setField(CandleQuarantineColumns.DETECTED_TS, detectedTsMs);
        row.setField(CandleQuarantineColumns.DETAIL,
                StringData.fromString(detail(instrumentToken, acc, window, reason)));
        row.setField(CandleQuarantineColumns.SCHEMA_VERSION_INDEX,
                StringData.fromString(CandleQuarantineColumns.SCHEMA_VERSION));
        return row;
    }

    /**
     * The offending candle as a compact, single-line JSON document — the
     * {@code raw_payload} evidence bytes. Hand-built (no serializer on the
     * emit path); field order is fixed so the bytes are deterministic for a
     * given candle.
     */
    private static byte[] evidence(long instrumentToken, CandleAccumulator acc, TimeWindow window,
            CandleInvariantCheck.Reason reason) {
        String json = "{\"instrument_token\":" + instrumentToken
                + ",\"exchange\":\"" + acc.exchange
                + "\",\"symbol\":\"" + acc.symbol
                + "\",\"window_start\":" + window.getStart()
                + ",\"window_end\":" + window.getEnd()
                + ",\"open_paise\":" + acc.openPaise
                + ",\"high_paise\":" + acc.highPaise
                + ",\"low_paise\":" + acc.lowPaise
                + ",\"close_paise\":" + acc.closePaise
                + ",\"volume\":" + acc.volume
                + ",\"tick_count\":" + acc.tickCount
                + ",\"reason\":\"" + reason.quarantineCode() + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /** Bounded, operator-readable detail — market data only, no secrets by construction. */
    private static String detail(long instrumentToken, CandleAccumulator acc, TimeWindow window,
            CandleInvariantCheck.Reason reason) {
        String d = "token=" + instrumentToken
                + " symbol=" + acc.symbol
                + " window_start=" + window.getStart()
                + " window_end=" + window.getEnd()
                + " open_paise=" + acc.openPaise
                + " high_paise=" + acc.highPaise
                + " low_paise=" + acc.lowPaise
                + " close_paise=" + acc.closePaise
                + " volume=" + acc.volume
                + " tick_count=" + acc.tickCount
                + " violated=" + reason.metricName();
        return d.length() > DETAIL_MAX_LENGTH ? d.substring(0, DETAIL_MAX_LENGTH) : d;
    }

    /** Best-effort SHA-256 hex of the evidence bytes (unreachable failure → empty). */
    static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            return ""; // unreachable: SHA-256 is mandated by the JCA spec
        }
    }

    /**
     * Observability consumer for {@link #OUTPUT}: counts each quarantined
     * candle into {@code compute.candles.invalid.total} and its per-reason
     * {@code compute.candles.invalid.<reason>} MetricGroup counter (the
     * plan's {@code compute.candles.invalid.*} telemetry — native
     * flink-metrics-otel export, same path as
     * {@code compute.candles.late.dropped}), logs one bounded WARN line per
     * violation, and forwards the row unchanged to the quarantine sink. No
     * keyed state, no timers, no Fluss RPC (CHG-022 rule).
     */
    public static final class CounterFunction extends ProcessFunction<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private transient Counter invalidTotal;
        private transient Map<CandleInvariantCheck.Reason, Counter> byReason;

        @Override
        public void open(OpenContext openContext) throws Exception {
            invalidTotal = getRuntimeContext().getMetricGroup()
                    .counter("compute.candles.invalid.total");
            byReason = new EnumMap<>(CandleInvariantCheck.Reason.class);
            for (CandleInvariantCheck.Reason r : CandleInvariantCheck.Reason.values()) {
                byReason.put(r, getRuntimeContext().getMetricGroup()
                        .counter("compute.candles.invalid." + r.metricName()));
            }
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out) {
            String code = row.getString(CandleQuarantineColumns.REASON).toString();
            CandleInvariantCheck.Reason reason = CandleInvariantCheck.Reason.fromQuarantineCode(code);
            invalidTotal.inc();
            if (reason != null) {
                byReason.get(reason).inc();
            }
            String detail = row.isNullAt(CandleQuarantineColumns.DETAIL)
                    ? "" : row.getString(CandleQuarantineColumns.DETAIL).toString();
            LOG.warn("candle-invariant: candle quarantined (reason={}, id={}) — {}",
                    code,
                    row.getString(CandleQuarantineColumns.QUARANTINE_ID),
                    detail);
            out.collect(row);
        }

        /** Counter-source accessor (tests): total invalid candles. */
        long invalidTotalCountForTest() {
            return invalidTotal == null ? 0L : invalidTotal.getCount();
        }

        /** Counter-source accessor (tests): invalid candles for one reason. */
        long invalidCountForTest(CandleInvariantCheck.Reason reason) {
            Counter c = byReason == null ? null : byReason.get(reason);
            return c == null ? 0L : c.getCount();
        }
    }
}
