package com.trading.compute.signaljob;

import com.trading.compute.telemetry.ComputeOtlpEmitter;
import java.io.Serializable;
import java.util.ArrayList;
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
 * Fingerprint deduplication (Signal dossier operator 2, REQ-FC-003),
 * state-authoritative since 2026-08-16 (DEC-038 superseded — see
 * {@code docs/08_implementation/04-signal-job.md} §Design B): the complete
 * 5-minute dedup set lives in THIS operator's Flink keyed state, check
 * pointed atomically with the source offset. There is no external store and
 * no query-on-miss — a cold start after checkpoint restore is correct by
 * construction, never an empty dedup set.
 *
 * <p>Keyed by {@code instrument_token}; per key the state is a map whose key
 * is {@code fingerprint_version + scope + event_fingerprint} and whose value
 * is {@code (first_seen, expiry)} — no raw bytes, no decoded fields, no
 * candle or candidate values, no event objects (dossier state contract). A
 * fingerprint seen again before its expiry is counted as a duplicate and
 * dropped; the first occurrence passes downstream. Once expired, a
 * re-arriving fingerprint is eligible again — expiry is exactly
 * {@code first_seen + DEDUP_TTL_MS} in event time.
 *
 * <p><b>State shape (unchanged from the DEC-038 build, so existing
 * checkpoints stay readable):</b> the {@code fingerprint-dedup} MapState
 * ({@code String → DedupEntry}) and the {@code fingerprint-dedup-expiry}
 * MapState ({@code Long → List<String>}, index {@code expiry -> state keys}).
 * Flink 2.2.1 has only processing-time state TTL, so entries expire via
 * explicit event-time timers (one per expiry bucket at
 * {@code first_seen + TTL}); the timer fires when the watermark reaches the
 * bucket's timestamp and deletes the bucket's entries from both maps.
 *
 * <p><b>Boundedness by construction, not by eviction:</b> entries = accepted
 * rate × TTL horizon. The DEC-038 bounded-cache machinery (eviction to
 * {@code DEDUP_CACHE_MAX_ENTRIES}/{@code DEDUP_CACHE_MAX_BYTES}, Fluss
 * query-on-miss rehydration, the durable-write side output, the wall-clock
 * cleanup pass) was removed on 2026-08-16 — the state IS authoritative and
 * eviction would be a correctness hole, not a bound.
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
     * restored size is folded in EXACTLY on its first post-restore row.
     */
    private transient long dedupCount;
    private transient long expiryIndexCount;
    private transient long bytesEstimate;

    /** Per-token tracked dedup size; replace-with-actual on first row per token. */
    private final Map<Long, Long> tokenStateCount = new java.util.HashMap<>();

    /** Per-token tracked expiry-bucket count; same replace semantics. */
    private final Map<Long, Long> tokenExpiryCount = new java.util.HashMap<>();

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

        getRuntimeContext().getMetricGroup().gauge("compute.dedup.state.count",
                (Gauge<Long>) () -> dedupCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.expiry.index.count",
                (Gauge<Long>) () -> expiryIndexCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.state.bytes.estimate",
                (Gauge<Long>) () -> bytesEstimate);
    }

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
        long token = ctx.getCurrentKey();
        // State key = version | scope | fingerprint. Scope is the instrument
        // token (the keyBy key) — explicit here so the state key contract is
        // self-contained rather than relying on the Flink key namespace alone.
        String stateKey = version + "|" + token + "|" + fingerprint;

        if (!tokenStateCount.containsKey(token)) {
            resyncTokenGauges(token);
        }

        long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
        long expiry = DedupExpiry.expiryMs(eventTime, config.dedupTtlMs());
        if (expiry < eventTime) {
            // Event-time overflow boundary (P5.3): clamp to MAX_VALUE — the
            // fingerprint stays deduplicated for the whole runtime.
            expiry = Long.MAX_VALUE;
        }

        if (dedup.contains(stateKey)) {
            duplicates.inc();
            return;
        }

        // First occurrence (or expired) — accept and record. The state IS the
        // authority; there is no eviction and no external store to consult.
        cachePut(stateKey, eventTime, expiry, ctx);
        firstEvents.inc();
        out.collect(row);
    }

    /** Insert into the authoritative state + expiry index; register the expiry timer. */
    private void cachePut(String stateKey, long eventTime, long expiry, Context ctx)
            throws Exception {
        dedup.put(stateKey, new DedupEntry(eventTime, expiry));

        List<String> keys = expiryIndex.get(expiry);
        if (keys == null) {
            keys = new ArrayList<>(1);
            expiryIndexCount++;
            bytesEstimate += PER_BUCKET_ESTIMATE_BYTES;
            ComputeOtlpEmitter.recordDedupExpiryIndexDelta(1L);
            ComputeOtlpEmitter.recordDedupBytesDelta(PER_BUCKET_ESTIMATE_BYTES);
        }
        keys.add(stateKey);
        // Put back explicitly — RocksDB-backed MapState returns a copy from
        // get(), so in-place mutation would be lost.
        expiryIndex.put(expiry, keys);
        ctx.timerService().registerEventTimeTimer(expiry);

        dedupCount++;
        bytesEstimate += PER_ENTRY_ESTIMATE_BYTES;
        ComputeOtlpEmitter.recordDedupStateDelta(1L);
        ComputeOtlpEmitter.recordDedupBytesDelta(PER_ENTRY_ESTIMATE_BYTES);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        long token = ctx.getCurrentKey();
        if (!tokenStateCount.containsKey(token)) {
            resyncTokenGauges(token);
        }
        // Event-time expiry: the timer fires when the watermark reached the
        // bucket's timestamp — delete the bucket's entries from both maps.
        if (ctx.timerService().currentWatermark() >= timestamp) {
            expireCacheBucket(timestamp, token);
        }
    }

    /**
     * Delete the state entry at {@code expiry} (its timer fired): the expiry
     * index entry and the dedup entry itself. With no eviction the bucket is
     * never empty when the timer fires (the timer is registered exactly when
     * the first key of a bucket is inserted and deleted exactly when the last
     * is removed), so this is a plain delete.
     */
    private void expireCacheBucket(long timestamp, long token) throws Exception {
        List<String> keys = expiryIndex.get(timestamp);
        if (keys == null) {
            return;
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
