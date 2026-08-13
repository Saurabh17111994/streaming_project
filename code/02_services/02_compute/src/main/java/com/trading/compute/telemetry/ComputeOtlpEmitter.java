package com.trading.compute.telemetry;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal OTLP metrics emitter for the Signal job — ships the
 * {@code compute.invalid.byReason.schema-version} rejection counter to the
 * OpenTelemetry Collector at {@code http://<OTEL_COLLECTOR_HOST>:4318/v1/metrics}
 * (HTTP OTLP) so OpenObserve can alert on producer/consumer schema-version
 * drift (process rule 2, approved 2026-08-10).
 *
 * <p>Mirrors the ingestion {@code OtlpMetricsEmitter} pattern: zero external
 * dependencies (JDK HttpURLConnection + hand-built JSON), 10-second daemon
 * flush. The counter is incremented by {@code RawValidationFunction} only when
 * a row is rejected with reason {@code schema-version}.
 *
 * <p><b>Delta semantics (deliberate deviation from the ingestion cumulative
 * pattern).</b> Each flush emits the rejections counted since the previous
 * flush ({@code getAndSet(0)}) as a DELTA, non-monotonic sum. The O2 alert
 * {@code value > 0} therefore fires on <em>new</em> rejections in the window
 * — never on historical replay (the live run's legacy "1"-labeled rows must
 * not re-fire the alert after a checkpoint restore).
 *
 * <p>Single-JVM scope: the live/dev run embeds the whole Flink job in one
 * process, so a static counter incremented from operator threads and drained
 * by the emitter thread is exact. A distributed run (task managers) would need
 * a real Flink metrics reporter instead — out of scope for this pass.
 */
public final class ComputeOtlpEmitter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeOtlpEmitter.class);

    /**
     * Exact OTLP metric name — matches the Flink metric group+counter
     * {@code compute.invalid.byReason} / {@code schema-version}. OpenObserve
     * renames '.' to '_', so the stream is
     * {@code compute_invalid_byReason_schema-version}.
     */
    public static final String SCHEMA_VERSION_REJECTED_METRIC =
            "compute.invalid.byReason.schema-version";

    /**
     * Startup-mode gauge (CANDLE-KV-REPLAY-001 A3.4): 0 = restored from
     * checkpoint, 1 = explicit full replay. Ships alongside the rejection
     * counter in every flush so the operator can see, post hoc, which startup
     * mode a run used even after the log rotated.
     */
    public static final String STARTUP_MODE_METRIC = "compute.startup.mode";

    /**
     * Source idle-at-tail episode counter (tracker 14 P7/P10 — 2026-08-13
     * misdiagnosis lesson): incremented once per idle EPISODE by the
     * {@code SourceIdleWatchdogGenerator} watermark-level watchdog inside the
     * source operator when no raw record has been consumed for
     * {@code SOURCE_IDLE_ALERT_MS}. DELTA non-monotonic, like the rejection
     * counter — fires on NEW idle episodes, never on replay. OpenObserve
     * renames '.' to '_', so the stream is {@code compute_source_idle_at_tail};
     * alert {@code value > 0} means "a restored/stopped source sat idle at a
     * frozen feed tail" (correct idle-tail behavior — NOT a stall — so the
     * alert is an observability signal, not a failure).
     */
    public static final String SOURCE_IDLE_AT_TAIL_METRIC = "compute.source.idle.at.tail";

    /**
     * Dedup-state gauges (tracker 14 P5.1). Mirrors of the
     * {@code FingerprintDedupFunction} state sizes, resynced to the actual
     * {@code MapState} contents at operator {@code open()} (which runs AFTER
     * checkpoint restore, so a restored run's mirrors are exact from the
     * first flush) and kept current by O(1) deltas on insert/expiry. GAUGE
     * points — current value, not delta. Single-JVM scope like the other
     * mirrors (the embedded dev run has one task; distributed task managers
     * would need a real Flink reporter — documented limitation).
     */
    public static final String DEDUP_STATE_COUNT_METRIC = "compute.dedup.state.count";
    public static final String DEDUP_EXPIRY_INDEX_COUNT_METRIC =
            "compute.dedup.expiry.index.count";
    public static final String DEDUP_STATE_BYTES_METRIC = "compute.dedup.state.bytes.estimate";

    /** Incremented by RawValidationFunction; drained (delta) by the flush thread. */
    private static final AtomicLong SCHEMA_VERSION_REJECTED = new AtomicLong();

    /**
     * Incremented by SourceIdleWatchdogGenerator (once per idle episode);
     * drained (delta) by the flush thread.
     */
    private static final AtomicLong SOURCE_IDLE_AT_TAIL = new AtomicLong();

    /** Startup mode; -1 = never recorded (not yet started). */
    private static final AtomicLong STARTUP_MODE = new AtomicLong(-1L);

    /** Dedup-state gauge mirrors (tracker 14 P5.1); 0 until the operator resyncs. */
    private static final AtomicLong DEDUP_STATE_COUNT = new AtomicLong();
    private static final AtomicLong DEDUP_EXPIRY_INDEX_COUNT = new AtomicLong();
    private static final AtomicLong DEDUP_STATE_BYTES = new AtomicLong();

    /**
     * Extra resource attributes (tracker 14 P8.0/831), configured once at job
     * startup via {@link #configureResourceAttributes(String...)}: flat
     * {@code key1,value1,key2,value2,...} pairs appended after the fixed
     * {@code service.name}/{@code service.instance.id} attributes. Empty until
     * configured (unit tests never configure → payload shape unchanged).
     * Volatile so the config write from the job thread is visible to the
     * flush thread; values are static, immutable-after-configure strings.
     */
    private static volatile String[] extraResourceAttributes = new String[0];

    private final String collectorUrl;
    private final ScheduledExecutorService scheduler;

    /**
     * Configures the extra resource attributes (tracker 14 P8.0/831):
     * environment, host, deployment version, job name, execution mode — never
     * credentials (the caller passes only known-safe config fields). Must be
     * called before the first flush (from {@code SignalJob.run}, before
     * {@link #start()}). An odd argument count fails fast.
     */
    public static void configureResourceAttributes(String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "resource attributes must be key,value pairs — got " + keyValuePairs.length
                            + " arguments");
        }
        extraResourceAttributes = keyValuePairs.clone();
    }

    /** TEST-ONLY: resets the configured extras to none. */
    static void resetResourceAttributesForTest() {
        extraResourceAttributes = new String[0];
    }

    /** TEST-ONLY: returns the startup mode to its unrecorded (-1) state. */
    static void resetStartupModeForTest() {
        STARTUP_MODE.set(-1L);
    }

    public ComputeOtlpEmitter(String collectorHostPort) {
        this.collectorUrl = "http://" + collectorHostPort + "/v1/metrics";
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "compute-otlp-flush");
            t.setDaemon(true);
            return t;
        });
    }

    /** Called by {@code RawValidationFunction} on each schema-version rejection. */
    public static void recordSchemaVersionRejection() {
        SCHEMA_VERSION_REJECTED.incrementAndGet();
    }

    /**
     * Records the run's startup mode once, before the emitter starts flushing:
     * 0 = {@code StartupMode.RESTORE}, 1 = {@code StartupMode.FULL_REPLAY}.
     * Any other value fails fast — the gauge must never carry an unlabeled
     * startup.
     */
    public static void recordStartupMode(int mode) {
        if (mode != 0 && mode != 1) {
            throw new IllegalArgumentException(
                    "startup mode must be 0 (RESTORE) or 1 (FULL_REPLAY), got " + mode
                            + " (CANDLE-KV-REPLAY-001 A3.4)");
        }
        STARTUP_MODE.set(mode);
    }

    /**
     * Resets the dedup-state gauge mirrors to zero. TEST-ONLY hook (public:
     * cross-package dedup tests call it): the mirrors are JVM-wide statics
     * scoped to ONE running job; independent harnesses in one test JVM must
     * not leak counts across tests.
     */
    public static void resetDedupGaugesForTest() {
        DEDUP_STATE_COUNT.set(0L);
        DEDUP_EXPIRY_INDEX_COUNT.set(0L);
        DEDUP_STATE_BYTES.set(0L);
    }

    /** O(1) mirror delta on dedup-state insert (tracker 14 P5.1). */
    public static void recordDedupStateDelta(long delta) {
        DEDUP_STATE_COUNT.addAndGet(delta);
    }

    /** O(1) mirror delta on expiry-index bucket insert/remove (tracker 14 P5.1). */
    public static void recordDedupExpiryIndexDelta(long delta) {
        DEDUP_EXPIRY_INDEX_COUNT.addAndGet(delta);
    }

    /** O(1) mirror delta on the bytes estimate (tracker 14 P5.1). */
    public static void recordDedupBytesDelta(long delta) {
        DEDUP_STATE_BYTES.addAndGet(delta);
    }

    /** Current dedup-state gauge values (static: mirrors are JVM-wide, read cross-package). */
    public static long dedupStateCount() {
        return DEDUP_STATE_COUNT.get();
    }

    public static long dedupExpiryIndexCount() {
        return DEDUP_EXPIRY_INDEX_COUNT.get();
    }

    public static long dedupBytesEstimate() {
        return DEDUP_STATE_BYTES.get();
    }

    /** Current startup-mode gauge value; -1 if never recorded (package-visible for tests). */
    long startupModeValue() {
        return STARTUP_MODE.get();
    }

    /**
     * Deltas the static counter since the last drain (package-visible for the
     * drain-contract test; the flush thread is the only caller in production).
     */
    long drainDelta() {
        return SCHEMA_VERSION_REJECTED.getAndSet(0);
    }

    /**
     * Called by {@code SourceIdleWatchdogGenerator} once per source idle-at-tail
     * episode. NEVER called from the source task's processing-time thread in a
     * blocking way — this is a nanosecond static increment, safe on
     * {@code onPeriodicEmit} (the 200 ms FLIP-27 watermark timer). The WARN log
     * is emitted in the watchdog; only the delta lands here, drained by the
     * 10 s flush thread.
     */
    public static void recordSourceIdleAtTail() {
        SOURCE_IDLE_AT_TAIL.incrementAndGet();
    }

    /** Deltas the source idle-at-tail counter (public: cross-package watchdog tests read it). */
    public long drainSourceIdleAtTailDelta() {
        return SOURCE_IDLE_AT_TAIL.getAndSet(0);
    }

    /** Start periodic flush (every 10 s). Call once from {@code SignalJob.run}. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::flush, 10, 10, TimeUnit.SECONDS);
        LOG.info("compute-otlp: started (collector={}, metric={}, interval=10s)",
                collectorUrl, SCHEMA_VERSION_REJECTED_METRIC);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        flush(); // one last delta so a shutdown-time burst is not lost
        LOG.info("compute-otlp: closed");
    }

    void flush() {
        try {
            flushOnce();
        } catch (Exception e) {
            // Collector down must never fail the job — telemetry is off the
            // critical path (observability dossier contract).
            LOG.debug("compute-otlp: flush failed (collector unreachable?): {}", e.getMessage());
        }
    }

    /**
     * One POST with the deltas drained since the previous flush; returns the
     * collector's HTTP status. Test hook (P8.2 delivery/outage proof) and the
     * scheduled path both drain BEFORE posting — a failed export never
     * retries a stale delta (no duplicate/unbounded buffering; the collector
     * owns retry with bounded backoff).
     */
    public int flushOnce() throws java.io.IOException {
        long delta = drainDelta();
        long sourceIdleAtTail = drainSourceIdleAtTailDelta();
        String json = buildMetricsJson(delta, sourceIdleAtTail);
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
            // Collector down must never fail the job — telemetry is off the
            // critical path (observability dossier contract).
            LOG.warn("compute-otlp: flush HTTP {} (collector={})", code, collectorUrl);
        }
        conn.disconnect();
        return code;
    }

    /**
     * Synchronous OTLP/HTTP log emit for the {@code trading_alerts} stream
     * (tracker 14 P8.0 box 828). Used for deterministic, user-code-observable
     * lifecycle events — schema-preflight failure, full-replay start, restore
     * start — that happen on the submitting client BEFORE {@code env.execute}
     * (a periodic flush would never run). Best-effort: a collector outage must
     * never fail the job, so any failure is logged and swallowed.
     *
     * <p>Runtime failure events (checkpoint failure, restart, sink failure)
     * are NOT hooked here — Flink 2.2.1 exposes no user-code checkpoint-
     * failure/restart callback; those are covered by the O2 alert rules
     * (metrics path) + the flink_logs stream (documented in tracker 14).
     *
     * @param collectorHostPort host:port of the collector (OTLP HTTP 4318)
     * @param severity INFO/WARN/ERROR (maps to OTLP severity text + number)
     * @param event stable event name, e.g. {@code startup-mode}
     * @param detail human-readable one-line detail (no credentials)
     */
    public static void emitAlertLog(String collectorHostPort, String severity,
            String event, String detail) {
        String json = buildLogsJson(severity, event, detail);
        try {
            URL url = URI.create("http://" + collectorHostPort + "/v1/logs").toURL();
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
                // Collector down must never fail the job — telemetry is off
                // the critical path (observability dossier contract).
                LOG.warn("compute-otlp: alert log HTTP {} (collector={}, event={})",
                        code, collectorHostPort, event);
            }
            conn.disconnect();
        } catch (Exception e) {
            LOG.debug("compute-otlp: alert log emit failed (collector unreachable?): {}",
                    e.getMessage());
        }
    }

    /**
     * OTLP/JSON logs payload: one log record with the event name, severity,
     * and detail plus the same resource attributes as the metrics payload
     * (service.name/instance.id + configured extras). Package-visible for the
     * payload-shape tests.
     */
    static String buildLogsJson(String severity, String event, String detail) {
        long now = System.currentTimeMillis() * 1_000_000L; // epoch nanos
        int severityNumber = switch (severity) {
            case "INFO" -> 9;
            case "WARN" -> 13;
            case "ERROR" -> 17;
            default -> 9;
        };
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[");
        appendAttr(sb, "service.name", "compute");
        appendAttr(sb, "service.instance.id", "signal-job");
        String[] extras = extraResourceAttributes;
        for (int i = 0; i < extras.length; i += 2) {
            appendAttr(sb, extras[i], extras[i + 1]);
        }
        sb.setLength(sb.length() - 1); // drop trailing comma after last attribute
        sb.append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"compute\"},\"logRecords\":[")
          .append("{\"timeUnixNano\":\"").append(now).append("\",")
          .append("\"severityText\":\"").append(escapeJson(severity)).append("\",")
          .append("\"severityNumber\":").append(severityNumber).append(",")
          .append("\"body\":{\"stringValue\":\"").append(escapeJson(detail)).append("\"},")
          .append("\"attributes\":[");
        appendAttr(sb, "event", event);
        appendAttr(sb, "severity", severity);
        sb.setLength(sb.length() - 1); // drop trailing comma
        sb.append("]}]}]}]}");
        return sb.toString();
    }

    /**
     * OTLP/JSON payload: one DELTA non-monotonic sum for the rejection counter
     * plus (when the startup mode was recorded) a GAUGE for the run's startup
     * mode.
     */
    String buildMetricsJson(long delta, long sourceIdleAtTail) {
        long now = System.currentTimeMillis() * 1_000_000L; // epoch nanos
        long mode = STARTUP_MODE.get();
        StringBuilder sb = new StringBuilder(640);
        sb.append("{\"resourceMetrics\":[{\"resource\":{\"attributes\":[");
        appendAttr(sb, "service.name", "compute");
        appendAttr(sb, "service.instance.id", "signal-job");
        String[] extras = extraResourceAttributes;
        for (int i = 0; i < extras.length; i += 2) {
            appendAttr(sb, extras[i], extras[i + 1]);
        }
        sb.setLength(sb.length() - 1); // drop trailing comma after last attribute
        sb.append("]},\"scopeMetrics\":[{\"scope\":{\"name\":\"compute\"},\"metrics\":[");
        sb.append("{\"name\":\"").append(SCHEMA_VERSION_REJECTED_METRIC).append("\",")
          .append("\"unit\":\"rejections\",")
          .append("\"sum\":{\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\",")
          .append("\"isMonotonic\":false,")
          .append("\"dataPoints\":[{\"asInt\":").append(delta).append(",")
          .append("\"timeUnixNano\":\"").append(now).append("\"}]}}");
        sb.append(",{\"name\":\"").append(SOURCE_IDLE_AT_TAIL_METRIC).append("\",")
          .append("\"unit\":\"episodes\",")
          .append("\"sum\":{\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\",")
          .append("\"isMonotonic\":false,")
          .append("\"dataPoints\":[{\"asInt\":").append(sourceIdleAtTail).append(",")
          .append("\"timeUnixNano\":\"").append(now).append("\"}]}}");
        if (mode != -1L) {
            sb.append(",{\"name\":\"").append(STARTUP_MODE_METRIC).append("\",")
              .append("\"unit\":\"mode\",")
              .append("\"gauge\":{\"dataPoints\":[{\"asInt\":").append(mode).append(",")
              .append("\"timeUnixNano\":\"").append(now).append("\"}]}}");
        }
        // Dedup-state gauges (tracker 14 P5.1): current values, resynced at
        // operator open() and kept current by O(1) deltas.
        appendGauge(sb, DEDUP_STATE_COUNT_METRIC, DEDUP_STATE_COUNT.get(), "entries", now);
        appendGauge(sb, DEDUP_EXPIRY_INDEX_COUNT_METRIC, DEDUP_EXPIRY_INDEX_COUNT.get(),
                "buckets", now);
        appendGauge(sb, DEDUP_STATE_BYTES_METRIC, DEDUP_STATE_BYTES.get(), "bytes", now);
        sb.append("]}]}]}");
        return sb.toString();
    }

    private static void appendGauge(StringBuilder sb, String name, long value, String unit,
            long now) {
        sb.append(",{\"name\":\"").append(name).append("\",")
          .append("\"unit\":\"").append(unit).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":").append(value).append(",")
          .append("\"timeUnixNano\":\"").append(now).append("\"}]}}");
    }

    private static void appendAttr(StringBuilder sb, String key, String value) {
        sb.append("{\"key\":\"").append(escapeJson(key)).append("\",")
          .append("\"value\":{\"stringValue\":\"").append(escapeJson(value)).append("\"}},");
    }

    /**
     * Minimal JSON string escaping (quotes, backslash, control chars) — the
     * configured extras may carry hostnames/env values; an unescaped quote
     * would corrupt the whole OTLP payload.
     */
    private static String escapeJson(String s) {
        StringBuilder out = null;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            String esc;
            switch (c) {
                case '"' -> esc = "\\\"";
                case '\\' -> esc = "\\\\";
                case '\b' -> esc = "\\b";
                case '\f' -> esc = "\\f";
                case '\n' -> esc = "\\n";
                case '\r' -> esc = "\\r";
                case '\t' -> esc = "\\t";
                default -> {
                    if (c < 0x20) {
                        esc = String.format("\\u%04x", (int) c);
                    } else {
                        esc = null;
                    }
                }
            }
            if (esc != null) {
                if (out == null) {
                    out = new StringBuilder(s.length() + 16);
                    out.append(s, 0, i);
                }
                out.append(esc);
            } else if (out != null) {
                out.append(c);
            }
        }
        return out == null ? s : out.toString();
    }
}
