package com.trading.compute.signaljob;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Bounded fingerprint deduplication (Signal dossier operator 2, REQ-FC-003).
 *
 * <p>Keyed by {@code instrument_token}; per key the state is a map whose key
 * is {@code fingerprint_version + scope + event_fingerprint} and whose value is
 * {@code (first_seen, expiry)} — no raw bytes, no decoded fields, no candle or
 * candidate values, no event objects (dossier state contract). A fingerprint
 * seen again before its expiry is counted as a duplicate and dropped; the
 * first occurrence passes downstream. Once expired, a re-arriving fingerprint
 * is eligible again — expiry is exactly {@code first_seen + DEDUP_TTL_MS} in
 * event time.
 *
 * <p><b>Expiry timer design.</b> The dossier acceptance requires the duplicate
 * state to be "absent after its expiry timer runs" (04-signal-job.md). Flink
 * 2.2.1 removed event-time state TTL (only {@code ProcessingTime} remains in
 * {@code StateTtlConfig.TtlTimeCharacteristic} — verified against flink-core
 * 2.2.1), so expiry is enforced with explicit event-time timers: one timer per
 * fingerprint entry registered at {@code first_seen + TTL}, plus a compact
 * expiry index ({@code expiry -> state keys}) so {@link #onTimer} knows which
 * fingerprints to delete. The index is one list entry per live fingerprint —
 * bounded with the dedup state itself, roughly doubling the dossier's raw
 * per-entry estimate (documented delta; still ~1.3 GB at the deferred
 * 60,000 t/s envelope on 48 GB VMs).
 *
 * <p><b>Deletion timing.</b> An event-time timer fires when the watermark
 * reaches the expiry instant. With the bounded-out-of-orderness watermark
 * (max seen event time − 5 s), that is at most {@code outOfOrderMs} of
 * event-time behind nominal expiry — entries are never deleted early, and the
 * logical expiry instant is {@code first_seen + TTL} exactly.
 */
public class FingerprintDedupFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final SignalJobConfig config;

    private transient MapState<String, DedupEntry> dedup;
    private transient MapState<Long, List<String>> expiryIndex;
    private transient Counter firstEvents;
    private transient Counter duplicates;

    /**
     * Gauge state (tracker 14 P5.1): live sizes of the two MapStates plus a
     * conservative bytes estimate. Keyed {@code MapState} cannot be iterated
     * in {@code open()} (no key namespace — verified: the harness throws "No
     * key set" on {@code entries()} outside a keyed callback), so each token's
     * restored size is folded in EXACTLY on its first post-restore row: the
     * per-token tracked contribution is replaced by the actual restored map
     * size (never added — restore + deltas stay exact), then O(1) deltas keep
     * it current. A token with restored state but no post-restore row and no
     * restored timer (a frozen market) is counted from its first row; the
     * gauge converges to exact as live tokens pass. Exposed as Flink gauges
     * (live-queryable) and mirrored to OTLP by {@link ComputeOtlpEmitter}.
     */
    private transient long dedupCount;
    private transient long expiryIndexCount;
    private transient long bytesEstimate;

    /** Per-token tracked dedup size; replace-with-actual on first row per token. */
    private final Map<Long, Long> tokenStateCount = new HashMap<>();

    /** Per-token tracked expiry-bucket count; same replace semantics. */
    private final Map<Long, Long> tokenExpiryCount = new HashMap<>();

    /**
     * Upper-bound per-entry estimate: state key ({@code version|token|fingerprint},
     * ~40 chars UTF-16 = 80 B) + {@link DedupEntry} (2 longs = 16 B) + MapState
     * overhead (~32 B). Measured upper bound — see
     * {@code DedupStateSizeTest} for the serialized-size check.
     */
    static final long PER_ENTRY_ESTIMATE_BYTES = 128L;

    /** Upper-bound per-expiry-bucket estimate: Long key + List<String> + overhead. */
    static final long PER_BUCKET_ESTIMATE_BYTES = 64L;

    public FingerprintDedupFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        dedup = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("fingerprint-dedup", Types.STRING,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(DedupEntry.class)));
        expiryIndex = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("fingerprint-dedup-expiry", Types.LONG, Types.LIST(Types.STRING)));

        firstEvents = getRuntimeContext().getMetricGroup().counter("compute.dedup.first");
        duplicates = getRuntimeContext().getMetricGroup().counter("compute.dedup.duplicates");

        // Tracker 14 P5.1: register the Flink gauges. The counts themselves
        // are maintained by the per-token replace-resync + O(1) deltas in
        // processElement/onTimer — open() must NOT touch keyed state (no key
        // namespace outside a keyed callback).
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.state.count",
                (Gauge<Long>) () -> dedupCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.expiry.index.count",
                (Gauge<Long>) () -> expiryIndexCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.state.bytes.estimate",
                (Gauge<Long>) () -> bytesEstimate);
    }

    /**
     * Folds a token's RESTORED (or first-seen) state into the gauge mirrors
     * exactly: the tracked contribution is REPLACED by the actual MapState
     * size, so restore + deltas never double-count (tracker 14 P5.1). Called
     * once per token, on its first processElement/timer after (re)start.
     */
    private void resyncTokenGauges(long token) throws Exception {
        long tracked = tokenStateCount.getOrDefault(token, 0L);
        long actual = 0;
        for (java.util.Map.Entry<String, DedupEntry> ignored : dedup.entries()) {
            actual++;
        }
        long delta = actual - tracked;
        if (delta != 0) {
            dedupCount += delta;
            bytesEstimate += delta * PER_ENTRY_ESTIMATE_BYTES;
            ComputeOtlpEmitter.recordDedupStateDelta(delta);
            ComputeOtlpEmitter.recordDedupBytesDelta(delta * PER_ENTRY_ESTIMATE_BYTES);
        }
        tokenStateCount.put(token, actual);

        long trackedExp = tokenExpiryCount.getOrDefault(token, 0L);
        long actualExp = 0;
        for (java.util.Map.Entry<Long, List<String>> ignored : expiryIndex.entries()) {
            actualExp++;
        }
        long deltaExp = actualExp - trackedExp;
        if (deltaExp != 0) {
            expiryIndexCount += deltaExp;
            bytesEstimate += deltaExp * PER_BUCKET_ESTIMATE_BYTES;
            ComputeOtlpEmitter.recordDedupExpiryIndexDelta(deltaExp);
            ComputeOtlpEmitter.recordDedupBytesDelta(deltaExp * PER_BUCKET_ESTIMATE_BYTES);
        }
        tokenExpiryCount.put(token, actualExp);
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out) throws Exception {
        String version = row.getString(RawTableColumns.FINGERPRINT_VERSION).toString();
        String fingerprint = row.getString(RawTableColumns.EVENT_FINGERPRINT).toString();
        // State key = version | scope | fingerprint. Scope is the instrument
        // token (the keyBy key) — explicit here so the state key contract is
        // self-contained rather than relying on the Flink key namespace alone.
        String stateKey = version + "|" + ctx.getCurrentKey() + "|" + fingerprint;

        // First row for this token after (re)start: fold its restored state
        // into the gauges exactly (replace, never add).
        if (!tokenStateCount.containsKey(ctx.getCurrentKey())) {
            resyncTokenGauges(ctx.getCurrentKey());
        }

        if (dedup.contains(stateKey)) {
            duplicates.inc();
            return;
        }

        long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
        long expiry = eventTime + config.dedupTtlMs();
        if (expiry < eventTime) {
            // Event-time overflow boundary (P5.3): a fingerprint whose nominal
            // expiry would wrap to a negative instant clamps to MAX_VALUE —
            // it stays deduplicated for the whole runtime instead of firing a
            // bogus immediate timer. Flink accepts the MAX_VALUE timer; it
            // only fires if the watermark ever reaches it (never in practice).
            expiry = Long.MAX_VALUE;
        }
        dedup.put(stateKey, new DedupEntry(eventTime, expiry));

        List<String> keys = expiryIndex.get(expiry);
        if (keys == null) {
            keys = new ArrayList<>(1);
            expiryIndexCount++;
            tokenExpiryCount.merge(ctx.getCurrentKey(), 1L, Long::sum);
            bytesEstimate += PER_BUCKET_ESTIMATE_BYTES;
            ComputeOtlpEmitter.recordDedupExpiryIndexDelta(1L);
            ComputeOtlpEmitter.recordDedupBytesDelta(PER_BUCKET_ESTIMATE_BYTES);
        }
        keys.add(stateKey);
        expiryIndex.put(expiry, keys);
        ctx.timerService().registerEventTimeTimer(expiry);

        dedupCount++;
        tokenStateCount.merge(ctx.getCurrentKey(), 1L, Long::sum);
        bytesEstimate += PER_ENTRY_ESTIMATE_BYTES;
        ComputeOtlpEmitter.recordDedupStateDelta(1L);
        ComputeOtlpEmitter.recordDedupBytesDelta(PER_ENTRY_ESTIMATE_BYTES);

        firstEvents.inc();
        out.collect(row);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
        List<String> keys = expiryIndex.get(timestamp);
        if (keys == null) {
            return;
        }
        long token = ctx.getCurrentKey();
        // A restored timer may be the token's first post-restore callback —
        // fold the restored state in before applying the deletion deltas.
        if (!tokenStateCount.containsKey(token)) {
            resyncTokenGauges(token);
        }
        for (String stateKey : keys) {
            dedup.remove(stateKey);
        }
        expiryIndex.remove(timestamp);
        dedupCount -= keys.size();
        expiryIndexCount--;
        bytesEstimate -= keys.size() * PER_ENTRY_ESTIMATE_BYTES + PER_BUCKET_ESTIMATE_BYTES;
        tokenStateCount.merge(token, -(long) keys.size(), Long::sum);
        tokenExpiryCount.merge(token, -1L, Long::sum);
        ComputeOtlpEmitter.recordDedupStateDelta(-keys.size());
        ComputeOtlpEmitter.recordDedupExpiryIndexDelta(-1L);
        ComputeOtlpEmitter.recordDedupBytesDelta(
                -(keys.size() * PER_ENTRY_ESTIMATE_BYTES + PER_BUCKET_ESTIMATE_BYTES));
    }

    /** Compact value: {@code (first_seen, expiry)} in event-time milliseconds. */
    public record DedupEntry(long firstSeenMs, long expiryMs) implements Serializable {}
}
