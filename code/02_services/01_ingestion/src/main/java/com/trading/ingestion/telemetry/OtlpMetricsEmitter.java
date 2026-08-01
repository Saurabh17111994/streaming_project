package com.trading.ingestion.telemetry;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight OTLP metrics emitter — accumulates metrics in-process
 * and periodically flushes to the OpenTelemetry Collector at
 * {@code http://otel-collector:4318/v1/metrics} (HTTP OTLP).
 *
 * <p>Uses zero external dependencies — pure JDK HttpURLConnection + JSON.
 * Counters and histograms use {@link AtomicLong}; gauges use
 * {@link DoubleAdder}. A background thread flushes every 10 seconds.
 *
 * <p>12 metrics per dossier:
 * <ol>
 *   <li>tick.throughput — counter (ticks/sec)</li>
 *   <li>tick.bytes — counter (bytes/sec)</li>
 *   <li>append.latency.ms — histogram (p50/p90/p99)</li>
 *   <li>append.pending.records — gauge</li>
 *   <li>append.pending.bytes — gauge</li>
 *   <li>bridge.reconnects — counter</li>
 *   <li>bridge.connected — gauge (0/1)</li>
 *   <li>manifest.version — gauge</li>
 *   <li>decode.errors — counter (by reason)</li>
 *   <li>fingerprint.count — counter</li>
 *   <li>clock.offset.ms — gauge</li>
 *   <li>ingestion.ready — gauge (0/1)</li>
 * </ol>
 */
public final class OtlpMetricsEmitter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpMetricsEmitter.class);

    private final String collectorUrl;
    private final String instanceId;
    private final String serviceName = "ingestion";
    private final ScheduledExecutorService scheduler;
    private volatile boolean closed;

    // ---- Counters ----
    private final AtomicLong tickCount = new AtomicLong(0);
    private final AtomicLong byteCount = new AtomicLong(0);
    private final AtomicLong bridgeReconnects = new AtomicLong(0);
    private final AtomicLong decodeErrors = new AtomicLong(0);
    private final AtomicLong fingerprintCount = new AtomicLong(0);
    private final AtomicLong acknowledgedLoss = new AtomicLong(0);
    private final AtomicLong heartbeatFailures = new AtomicLong(0);
    private final AtomicLong feedStalls = new AtomicLong(0);
    private final AtomicLong subscriptionRetries = new AtomicLong(0);
    private final AtomicLong partialSubscriptions = new AtomicLong(0);
    private final AtomicLong authRefreshes = new AtomicLong(0);
    private final AtomicLong authFailures = new AtomicLong(0);

    // ---- Histogram (approximate via linear buckets) ----
    private final AtomicLong appendLatencyTotalMs = new AtomicLong(0);
    private final AtomicLong appendLatencyCount = new AtomicLong(0);
    private final AtomicLong appendLatencyP50 = new AtomicLong(0);
    private final AtomicLong appendLatencyP90 = new AtomicLong(0);
    private final AtomicLong appendLatencyP99 = new AtomicLong(0);
    // Simple percentile tracking: ring buffer
    private final long[] latencyRing = new long[1024];
    private volatile int latencyRingPos;

    // ---- Gauges ----
    private volatile long pendingRecords;
    private volatile long pendingBytes;
    private volatile int bridgeConnected; // 0 or 1
    private volatile long manifestVersion;
    private volatile long clockOffsetMs;
    private volatile long connectionEpoch;
    private volatile int ingestionReady; // 0 or 1

    // ---- Reason counters ----
    private final ConcurrentMap<String, AtomicLong> decodeReasonCounters = new ConcurrentHashMap<>();

    // ---- Slot metrics (plan §Monitoring — labeled by slot only) ----
    private final ConcurrentMap<String, SlotMetricState> slotStates = new ConcurrentHashMap<>();

    /** Per-slot gauge snapshot. Never keyed by token/symbol. */
    public static final class SlotMetricState {
        public volatile int active;            // 0 or 1 (ACTIVE state)
        public volatile long assigned;
        public volatile long acknowledged;
        public volatile long rejected;
        public volatile long lastFrameNanos;
        public volatile double capacityUsedPercent;
    }

    // ---- Resource metrics (plan Amendment §Resource) ----
    private volatile long reconnectConsecutive;
    private volatile int activeSockets;
    private volatile int childProcessAlive;    // 0 or 1
    private volatile long processOpenFds;
    private volatile long processFdLimit;
    private volatile double processFdUsagePercent;
    private volatile long processRssBytes;
    private volatile long goGoroutines;
    private volatile long jvmThreadsLive;
    private volatile boolean otlpHealthy;       // most recent export success

    /** Invoked after each flush with success(true)/failure(false) — feeds telemetry readiness. */
    private volatile java.util.function.Consumer<Boolean> healthCallback;

    public OtlpMetricsEmitter(String collectorHostPort, String instanceId) {
        this.collectorUrl = "http://" + collectorHostPort + "/v1/metrics";
        this.instanceId = instanceId;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otlp-metrics-flush");
            t.setDaemon(true);
            return t;
        });
    }

    /** Set a callback invoked with flush success/failure (telemetry readiness feed). */
    public void setHealthCallback(java.util.function.Consumer<Boolean> callback) {
        this.healthCallback = callback;
    }

    /** Start periodic flush (every 10s). Call once after wiring all recording sites. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::flush, 10, 10, TimeUnit.SECONDS);
        LOG.info("otlp-metrics: started (collector={}, interval=10s)", collectorUrl);
    }

    @Override
    public void close() {
        closed = true;
        scheduler.shutdown();
        flush(); // final flush before shutdown
        LOG.info("otlp-metrics: closed");
    }

    // ---- Recording API ----

    public void recordTick(int byteSize) {
        tickCount.incrementAndGet();
        byteCount.addAndGet(byteSize);
    }

    public void recordAppendLatencyMs(long latencyMs) {
        appendLatencyTotalMs.addAndGet(latencyMs);
        appendLatencyCount.incrementAndGet();
        // Ring buffer for approximate percentiles
        int pos = latencyRingPos;
        if (pos < latencyRing.length) {
            latencyRing[pos] = latencyMs;
            latencyRingPos = pos + 1;
        }
    }

    public void setPendingRecords(long v) { this.pendingRecords = v; }
    public void setPendingBytes(long v) { this.pendingBytes = v; }
    public void incrementBridgeReconnects() { bridgeReconnects.incrementAndGet(); }
    public void setBridgeConnected(boolean v) { bridgeConnected = v ? 1 : 0; }
    public void setManifestVersion(long v) { manifestVersion = v; }
    public void incrementDecodeError(String reason) {
        decodeErrors.incrementAndGet();
        decodeReasonCounters.computeIfAbsent(reason, k -> new AtomicLong(0)).incrementAndGet();
    }
    public void incrementFingerprint() { fingerprintCount.incrementAndGet(); }
    public void setClockOffsetMs(long v) { clockOffsetMs = v; }
    public void setIngestionReady(boolean v) { ingestionReady = v ? 1 : 0; }
    public void incrementAcknowledgedLoss() { acknowledgedLoss.incrementAndGet(); }
    public void incrementHeartbeatFailure() { heartbeatFailures.incrementAndGet(); }
    public void incrementFeedStall() { feedStalls.incrementAndGet(); }
    public void incrementSubscriptionRetry() { subscriptionRetries.incrementAndGet(); }
    public void incrementPartialSubscription() { partialSubscriptions.incrementAndGet(); }
    public void incrementAuthRefresh() { authRefreshes.incrementAndGet(); }
    public void incrementAuthFailure() { authFailures.incrementAndGet(); }
    public void setConnectionEpoch(long v) { connectionEpoch = v; }

    // ---- Slot + resource recording (plan §Monitoring / Amendment §Resource) ----

    /** Snapshot per-slot coverage state. Slot label only — never token/symbol. */
    public void setSlotState(String slotId, boolean active, long assigned,
                             long acknowledged, long rejected, long lastFrameNanos) {
        SlotMetricState s = slotStates.computeIfAbsent(slotId, ignored -> new SlotMetricState());
        s.active = active ? 1 : 0;
        s.assigned = assigned;
        s.acknowledged = acknowledged;
        s.rejected = rejected;
        s.lastFrameNanos = lastFrameNanos;
    }

    /** Capacity used percent for a slot (plan ING-CAP-001). */
    public void setSlotCapacityUsedPercent(String slotId, double percent) {
        SlotMetricState s = slotStates.computeIfAbsent(slotId, ignored -> new SlotMetricState());
        s.capacityUsedPercent = percent;
    }

    public void setReconnectConsecutive(long v) { reconnectConsecutive = v; }
    public void setActiveSockets(int v) { activeSockets = v; }
    public void setChildProcessAlive(boolean v) { childProcessAlive = v ? 1 : 0; }
    public void setProcessOpenFds(long v) { processOpenFds = v; }
    public void setProcessFdLimit(long v) { processFdLimit = v; }
    public void setProcessFdUsagePercent(double v) { processFdUsagePercent = v; }
    public void setProcessRssBytes(long v) { processRssBytes = v; }
    public void setGoGoroutines(long v) { goGoroutines = v; }
    public void setJvmThreadsLive(long v) { jvmThreadsLive = v; }

    // ---- package-private test accessors ----

    SlotMetricState slotState(String slotId) { return slotStates.get(slotId); }
    int childProcessAlive() { return childProcessAlive; }
    void forceFlush() { flush(); }

    // ---- Flush ----

    private void flush() {
        if (closed) return;
        try {
            String json = buildMetricsJson();
            if (json == null) return;

            URL url = URI.create(collectorUrl).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(5_000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            if (code >= 400) {
                LOG.warn("otlp-metrics: flush HTTP {} (url={})", code, collectorUrl);
            }
            conn.disconnect();
            reportHealth(true);

        } catch (Exception e) {
            LOG.debug("otlp-metrics: flush failed (collector may not be running): {}",
                    e.getMessage());
            reportHealth(false);
        }
    }

    private void reportHealth(boolean healthy) {
        this.otlpHealthy = healthy;
        java.util.function.Consumer<Boolean> cb = healthCallback;
        if (cb != null) {
            try {
                cb.accept(healthy);
            } catch (Exception ignore) { /* never let a callback break flushing */ }
        }
    }

    // ---- JSON builder (minimal OTLP metrics format) ----

    private String buildMetricsJson() {
        long now = System.currentTimeMillis() * 1_000_000L; // epoch nanos
        // Compute approximate percentiles from ring buffer
        computeLatencyPercentiles();

        StringBuilder sb = new StringBuilder(4096);
        sb.append("{\"resourceMetrics\":[{\"resource\":{\"attributes\":[");
        appendAttr(sb, "service.name", serviceName);
        appendAttr(sb, "service.instance.id", instanceId);
        sb.append("]},\"scopeMetrics\":[{\"scope\":{\"name\":\"ingestion\"},\"metrics\":[");

        // Counter: tick.throughput
        appendSum(sb, "tick.throughput", "ticks", tickCount.get(), now);
        appendSum(sb, "tick.bytes", "bytes", byteCount.get(), now);
        appendSum(sb, "bridge.reconnects", "reconnects", bridgeReconnects.get(), now);
        appendSum(sb, "decode.errors", "errors", decodeErrors.get(), now);
        appendSum(sb, "fingerprint.count", "fingerprints", fingerprintCount.get(), now);
        appendSum(sb, "append.acknowledged.loss", "records", acknowledgedLoss.get(), now);
        appendSum(sb, "heartbeat.failures", "failures", heartbeatFailures.get(), now);
        appendSum(sb, "feed.stalls", "stalls", feedStalls.get(), now);
        appendSum(sb, "subscription.retries", "retries", subscriptionRetries.get(), now);
        appendSum(sb, "subscription.partial", "events", partialSubscriptions.get(), now);
        appendSum(sb, "auth.refreshes", "refreshes", authRefreshes.get(), now);
        appendSum(sb, "auth.failures", "failures", authFailures.get(), now);

        // Histogram: append.latency.ms
        appendHistogram(sb, "append.latency.ms", "ms", appendLatencyCount.get(),
                appendLatencyTotalMs.get(), appendLatencyP50.get(),
                appendLatencyP90.get(), appendLatencyP99.get(), now);

        // Gauge: append.pending.records
        appendGaugeLong(sb, "append.pending.records", "records", pendingRecords, now);
        appendGaugeLong(sb, "append.pending.bytes", "bytes", pendingBytes, now);
        appendGaugeInt(sb, "bridge.connected", "1", bridgeConnected, now);
        appendGaugeLong(sb, "manifest.version", "version", manifestVersion, now);
        appendGaugeLong(sb, "clock.offset.ms", "ms", clockOffsetMs, now);
        appendGaugeLong(sb, "bridge.connection.epoch", "epoch", connectionEpoch, now);
        appendGaugeInt(sb, "ingestion.ready", "1", ingestionReady, now);

        // ---- Slot gauges (labeled by slot only) ----
        long nowNanos = now;
        slotStates.forEach((slotId, s) -> {
            long frameAgeMs = s.lastFrameNanos > 0
                    ? Math.max(0, (System.nanoTime() - s.lastFrameNanos) / 1_000_000L)
                    : -1;
            appendGaugeIntLabeled(sb, "bridge.slot.active", "1", s.active, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.assigned", "tokens", s.assigned, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.acknowledged", "tokens", s.acknowledged, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.rejected", "tokens", s.rejected, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.last_frame_age_ms", "ms", frameAgeMs, slotId, nowNanos);
            appendGaugeDoubleLabeled(sb, "bridge.slot.capacity_used_percent", "%",
                    s.capacityUsedPercent, slotId, nowNanos);
        });

        // ---- Resource + capacity gauges (Amendment §Resource) ----
        appendGaugeLong(sb, "bridge.reconnect.consecutive", "reconnects", reconnectConsecutive, now);
        appendGaugeInt(sb, "bridge.active_sockets", "sockets", activeSockets, now);
        appendGaugeInt(sb, "bridge.child_process_alive", "1", childProcessAlive, now);
        appendGaugeLong(sb, "process.open_fds", "fds", processOpenFds, now);
        appendGaugeLong(sb, "process.fd_limit", "fds", processFdLimit, now);
        appendGaugeDouble(sb, "process.fd_usage_percent", "%", processFdUsagePercent, now);
        appendGaugeLong(sb, "process.rss_bytes", "bytes", processRssBytes, now);
        appendGaugeLong(sb, "go.goroutines", "goroutines", goGoroutines, now);
        appendGaugeLong(sb, "jvm.threads.live", "threads", jvmThreadsLive, now);
        appendGaugeInt(sb, "otel.collector.healthy", "1", otlpHealthy ? 1 : 0, now);

        // Trim last comma
        sb.setLength(sb.length() - 1);

        sb.append("]}]}]}");
        return sb.toString();
    }

    private void computeLatencyPercentiles() {
        int count = Math.min(latencyRingPos, latencyRing.length);
        if (count == 0) return;
        // Copy and sort for percentiles
        long[] sorted = new long[count];
        System.arraycopy(latencyRing, 0, sorted, 0, count);
        java.util.Arrays.sort(sorted);
        appendLatencyP50.set(sorted[count / 2]);                    // p50
        appendLatencyP90.set(sorted[(int) (count * 0.90)]);         // p90
        appendLatencyP99.set(sorted[(int) (count * 0.99)]);         // p99
        // Reset ring buffer
        latencyRingPos = 0;
    }

    private void appendSum(StringBuilder sb, String name, String unit, long value, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"sum\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendHistogram(StringBuilder sb, String name, String unit,
                                  long count, long sum, long p50, long p90, long p99,
                                  long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"histogram\":{\"dataPoints\":[{")
          .append("\"count\":\"").append(count).append("\",")
          .append("\"sum\":").append(sum).append(",")
          .append("\"bucketCounts\":[\"0\",\"0\",\"0\",\"0\"],")
          .append("\"explicitBounds\":[0,0,0,0],")
          .append("\"attributes\":[{")
          .append("\"key\":\"p50\",\"value\":{\"intValue\":\"").append(p50).append("\"}},")
          .append("{\"key\":\"p90\",\"value\":{\"intValue\":\"").append(p90).append("\"}},")
          .append("{\"key\":\"p99\",\"value\":{\"intValue\":\"").append(p99).append("\"}}")
          .append("],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeLong(StringBuilder sb, String name, String unit,
                                  long value, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeInt(StringBuilder sb, String name, String unit,
                                 int value, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeDouble(StringBuilder sb, String name, String unit,
                                   double value, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asDouble\":\"").append(value).append("\",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeIntLabeled(StringBuilder sb, String name, String unit,
                                       int value, String slotId, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"attributes\":[{\"key\":\"slot\",\"value\":{\"stringValue\":\"")
          .append(esc(slotId)).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeLongLabeled(StringBuilder sb, String name, String unit,
                                        long value, String slotId, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"attributes\":[{\"key\":\"slot\",\"value\":{\"stringValue\":\"")
          .append(esc(slotId)).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeDoubleLabeled(StringBuilder sb, String name, String unit,
                                          double value, String slotId, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asDouble\":\"").append(value).append("\",")
          .append("\"attributes\":[{\"key\":\"slot\",\"value\":{\"stringValue\":\"")
          .append(esc(slotId)).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendAttr(StringBuilder sb, String key, String value) {
        sb.append("{\"key\":\"").append(key).append("\",")
          .append("\"value\":{\"stringValue\":\"").append(esc(value)).append("\"}},");
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
