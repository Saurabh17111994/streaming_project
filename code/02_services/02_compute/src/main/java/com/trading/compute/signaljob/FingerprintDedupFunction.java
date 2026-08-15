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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Bounded fingerprint deduplication (Signal dossier operator 2, REQ-FC-003),
 * re-worked for DEC-038 state ownership: the Fluss {@code fingerprint_dedup}
 * table is authoritative; this function's Flink state is only the bounded hot
 * cache (hard caps {@code DEDUP_CACHE_MAX_ENTRIES} / {@code DEDUP_CACHE_MAX_BYTES},
 * effective cap = min of the two) and is never a second copy.
 *
 * <p>Keyed by {@code instrument_token}; per key the cache is a map whose key
 * is {@code fingerprint_version + scope + event_fingerprint} and whose value
 * is {@code (first_seen, expiry)} — no raw bytes, no decoded fields, no candle
 * or candidate values, no event objects (dossier state contract). A
 * fingerprint seen again before its expiry is counted as a duplicate and
 * dropped; the first occurrence passes downstream. Once expired, a re-arriving
 * fingerprint is eligible again — expiry is exactly
 * {@code first_seen + DEDUP_TTL_MS} in event time.
 *
 * <p><b>DEC-038 authority.</b> On cache miss the decision is made against the
 * authoritative store ({@link FingerprintDedupStateStore#lookup} — query-on-miss,
 * lazy rehydration): a cold cache after restart is correct, never an empty
 * dedup set (SIG-STATE-003). First-seen fingerprints are emitted to the
 * {@link #DEDUP_WRITE_OUTPUT} side output for the batched durable upsert
 * ({@link FingerprintDedupWriterFunction} + {@code fingerprint_dedup} sink);
 * the read path never trusts the cache alone.
 *
 * <p><b>Cache expiry.</b> Flink 2.2.1 has only processing-time state TTL, so
 * cache entries expire via explicit event-time timers (one per entry at
 * {@code first_seen + TTL}, index {@code expiry -> state keys}).
 *
 * <p><b>Durable cleanup.</b> A processing-time timer at
 * {@code DEDUP_CLEANUP_INTERVAL_MS} per key drives the bounded re-entrant
 * cleanup pass against the store ({@link DedupExpiry#selectBatch} +
 * {@link FingerprintDedupStateStore#delete}) — Fluss 0.9.1 has no per-key TTL,
 * so stale rows are harmless (never a false "seen") and the pass bounds table
 * growth. Exact delete/ack semantics are evidence-gated (SIG-STATE-001).
 *
 * <p><b>Cache bound.</b> When an accept pushes the cache over
 * {@code DEDUP_CACHE_MAX_ENTRIES} or {@code DEDUP_CACHE_MAX_BYTES}, the
 * earliest-expiring entries are evicted from the cache (never from the store —
 * evicted fingerprints are re-decided authoritatively on the next occurrence).
 */
public class FingerprintDedupFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /**
     * First-seen rows for the durable {@code fingerprint_dedup} upsert
     * (fingerprint_dedup v1 layout: token, version, fingerprint,
     * first_seen_ms, expiry_ms, schema_version).
     */
    public static final OutputTag<RowData> DEDUP_WRITE_OUTPUT =
            new OutputTag<RowData>("fingerprint-dedup-write") {};

    /** Serializable store factory — the graph ships the factory, not the connection. */
    @FunctionalInterface
    public interface StoreFactory extends Serializable {
        FingerprintDedupStateStore create() throws Exception;
    }

    private final SignalJobConfig config;
    private final StoreFactory storeFactory;

    private transient FingerprintDedupStateStore dedupStore;

    private transient MapState<String, DedupEntry> dedup;
    private transient MapState<Long, List<String>> expiryIndex;
    private transient Counter firstEvents;
    private transient Counter duplicates;
    /** DEC-038 telemetry: hot-path cache decisions + authoritative-store failures. */
    private transient Counter cacheHits;
    private transient Counter cacheMisses;
    private transient Counter rehydrationFailures;

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
        this(config, InMemoryFingerprintDedupStateStore::new);
    }

    public FingerprintDedupFunction(SignalJobConfig config, StoreFactory storeFactory) {
        this.config = config;
        this.storeFactory = storeFactory;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        dedup = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("fingerprint-dedup", Types.STRING,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(DedupEntry.class)));
        expiryIndex = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("fingerprint-dedup-expiry", Types.LONG, Types.LIST(Types.STRING)));

        dedupStore = storeFactory.create();

        firstEvents = getRuntimeContext().getMetricGroup().counter("compute.dedup.first");
        duplicates = getRuntimeContext().getMetricGroup().counter("compute.dedup.duplicates");
        cacheHits = getRuntimeContext().getMetricGroup().counter(
                "compute.dedup.cache.hits");
        cacheMisses = getRuntimeContext().getMetricGroup().counter(
                "compute.dedup.cache.misses");
        rehydrationFailures = getRuntimeContext().getMetricGroup().counter(
                "compute.dedup.rehydration.failures");

        getRuntimeContext().getMetricGroup().gauge("compute.dedup.state.count",
                (Gauge<Long>) () -> dedupCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.expiry.index.count",
                (Gauge<Long>) () -> expiryIndexCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.state.bytes.estimate",
                (Gauge<Long>) () -> bytesEstimate);
    }

    @Override
    public void close() throws Exception {
        if (dedupStore != null) {
            dedupStore.close();
        }
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
        long nowWall = ctx.timerService().currentProcessingTime();

        if (dedup.contains(stateKey)) {
            duplicates.inc();
            cacheHits.inc();
            ComputeOtlpEmitter.recordDedupCacheHit();
            return;
        }

        // Cache miss — decide against the authoritative store (query-on-miss,
        // lazy rehydration). Never trust the cache alone (SIG-STATE-003). The
        // lookup IS the rehydration read: its duration is the rehydration
        // latency gauge; a failure is counted before the task fails closed
        // (never an empty dedup set).
        cacheMisses.inc();
        ComputeOtlpEmitter.recordDedupCacheMiss();
        FingerprintDedupStateStore.Lookup verdict;
        long lookupStartNanos = System.nanoTime();
        try {
            verdict = dedupStore.lookup(token, version, fingerprint, nowWall);
        } catch (Exception e) {
            rehydrationFailures.inc();
            ComputeOtlpEmitter.recordDedupRehydrationFailure();
            throw e;
        } finally {
            long lookupMs = (System.nanoTime() - lookupStartNanos) / 1_000_000L;
            ComputeOtlpEmitter.recordDedupRehydrationLatencyMs(lookupMs);
        }
        if (verdict == FingerprintDedupStateStore.Lookup.SEEN_LIVE) {
            duplicates.inc();
            // Warm the cache so the authoritative read is not repeated per row.
            cachePut(stateKey, eventTime, expiry, ctx);
            return;
        }

        // First occurrence (or expired) — accept, cache, and schedule the
        // durable first-seen write.
        cachePut(stateKey, eventTime, expiry, ctx);
        RowData write = GenericRowData.of(
                token,
                StringData.fromString(version),
                StringData.fromString(fingerprint),
                eventTime,
                expiry,
                StringData.fromString(FingerprintDedupTableColumns.SCHEMA_VERSION_V1));
        ctx.output(DEDUP_WRITE_OUTPUT, write);

        firstEvents.inc();
        out.collect(row);

        scheduleCleanup(ctx);
    }

    /** Insert into the bounded cache + expiry index; evict to the cap. */
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

        evictToBounds(ctx);
    }

    /**
     * Enforce {@code DEDUP_CACHE_MAX_ENTRIES} / {@code DEDUP_CACHE_MAX_BYTES}
     * (effective cap = min): evict the earliest-expiring cache entries until
     * under the cap. Eviction never touches the store — an evicted fingerprint
     * is re-decided authoritatively on its next occurrence (query-on-miss).
     *
     * <p><b>DEC-038 hard invariant (evidenced 2026-08-15, SIG-STATE-001):</b>
     * the evicted bucket's event-time timer is DELETED here. Without this,
     * every accepted fingerprint registers a timer that outlives its cache
     * entry, so the checkpoint's timer state grows with the total accepted
     * count (the Fluss dedup cardinality) instead of the bounded cache —
     * exactly the "checkpoint must not duplicate the durable set" violation
     * the contract forbids. Deleting the timer keeps checkpoint state bounded
     * by the cache cap; a re-arriving fingerprint inside its TTL re-registers
     * it via the authoritative query-on-miss path.
     */
    private void evictToBounds(Context ctx) throws Exception {
        long cap = Math.min(config.dedupCacheMaxEntries(), maxEntriesByBytes());
        if (cap <= 0) {
            return;
        }
        while (dedupCount > cap) {
            // earliest expiry bucket first
            long earliest = Long.MAX_VALUE;
            for (java.util.Map.Entry<Long, List<String>> e : expiryIndex.entries()) {
                if (e.getKey() < earliest) {
                    earliest = e.getKey();
                }
            }
            if (earliest == Long.MAX_VALUE) {
                break; // no buckets — nothing to evict
            }
            List<String> bucket = expiryIndex.get(earliest);
            if (bucket == null) {
                break;
            }
            int evicted = 0;
            for (String k : bucket) {
                if (dedup.contains(k)) {
                    dedup.remove(k);
                    evicted++;
                }
            }
            expiryIndex.remove(earliest);
            // Bounded checkpoint invariant (DEC-038): the bucket's timer has no
            // remaining cache entries — delete it so timer state stays ≤ cache
            // cap and the checkpoint never mirrors Fluss dedup cardinality.
            ctx.timerService().deleteEventTimeTimer(earliest);
            dedupCount -= evicted;
            expiryIndexCount--;
            bytesEstimate -= evicted * PER_ENTRY_ESTIMATE_BYTES + PER_BUCKET_ESTIMATE_BYTES;
            ComputeOtlpEmitter.recordDedupStateDelta(-evicted);
            ComputeOtlpEmitter.recordDedupExpiryIndexDelta(-1L);
            ComputeOtlpEmitter.recordDedupBytesDelta(
                    -(evicted * PER_ENTRY_ESTIMATE_BYTES + PER_BUCKET_ESTIMATE_BYTES));
        }
    }

    private long maxEntriesByBytes() {
        long perEntryWithShare = PER_ENTRY_ESTIMATE_BYTES + PER_BUCKET_ESTIMATE_BYTES;
        if (perEntryWithShare <= 0) {
            return Long.MAX_VALUE;
        }
        return Math.max(1L, config.dedupCacheMaxBytes() / perEntryWithShare);
    }

    /**
     * Register the durable cleanup timer for this key at the next
     * grid-aligned boundary: all accepted events within one interval register
     * the SAME processing-time timestamp, so Flink keeps exactly one cleanup
     * timer per key per {@code DEDUP_CLEANUP_INTERVAL_MS} — periodic cadence
     * with ZERO extra keyed state (the two-map compactness contract holds).
     */
    private void scheduleCleanup(Context ctx) {
        long now = ctx.timerService().currentProcessingTime();
        long interval = Math.max(1L, config.dedupCleanupIntervalMs());
        long at = interval * (now / interval + 1);
        if (at > now) {
            ctx.timerService().registerProcessingTimeTimer(at);
        }
    }

    /**
     * Bounded re-entrant cleanup pass for this key's token bucket: select the
     * expired rows ({@link DedupExpiry#selectBatch}) and delete them from the
     * store. Per-key cadence — keys without traffic are bounded by construction
     * (entries = accepted rate x TTL horizon), and the pass resumes on the next
     * tick if interrupted (re-entrant selection).
     */
    /** Bounded per-tick cleanup batch (per key, per cadence tick). */
    static final int CLEANUP_BATCH_SIZE = 1000;

    private void runCleanup(long token, long nowWall) throws Exception {
        List<DedupExpiry.CleanupCandidate> batch = dedupStore.scanExpired(
                token, nowWall, CLEANUP_BATCH_SIZE);
        if (!batch.isEmpty()) {
            dedupStore.delete(batch);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        long token = ctx.getCurrentKey();
        if (!tokenStateCount.containsKey(token)) {
            resyncTokenGauges(token);
        }

        // Route by timer domain: an event-time timer fires when the watermark
        // reached its timestamp (the cache-expiry index); a processing-time
        // timer fires when the wall clock reached it (the durable cleanup
        // cadence). Prefer the watermark test so a both-due collision (a
        // stalled source with the wall clock past an event-time expiry) still
        // performs the cache deletion.
        if (ctx.timerService().currentWatermark() >= timestamp) {
            expireCacheBucket(timestamp, token);
        } else {
            // Cleanup grid timer fired. The next accepted event registers the
            // next boundary; a frozen key simply stops cleaning (its store rows
            // are bounded by construction and never a false "seen").
            runCleanup(token, ctx.timerService().currentProcessingTime());
        }
    }

    /**
     * Delete the cache entry at {@code expiry} (its timer fired): the expiry
     * index entry and the dedup entry itself. If the entry was already evicted
     * by the bounded cache, the index is empty and this is a no-op.
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
