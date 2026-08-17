package com.trading.compute.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** OTLP payload contract for the rejection counter (process rule 2), source-idle episodes, and dedup gauges. */
@DisplayName("ComputeOtlpEmitter JSON shape")
class ComputeOtlpEmitterTest {

    @BeforeEach
    void clearJvmWideRejectionCounters() {
        // Live-cluster integration tests (gate COMPUTE_INT_TEST_P6=true) legitimately
        // record schema-version rejections into the same JVM-wide statics (their
        // fp-bad-schema rows), and the periodic emitter flush never runs in the test
        // JVM. Start every test from a clean delta window so ordering vs those tests
        // cannot break the "no increments yet -> 0" assertion. The startup-mode
        // static is order-sensitive too: a live job (or another test class) may
        // have already recorded a mode, which would make the "gauge absent before
        // recording" assertion fail depending on class ordering (seen in the
        // container suite).
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        emitter.drainDelta();
        emitter.drainSourceIdleAtTailDelta();
        emitter.drainSignalKvFilteredNonCanonicalDelta();
        emitter.drainCandleLateDropDelta();
        ComputeOtlpEmitter.resetStartupModeForTest();
        ComputeOtlpEmitter.resetDedupTelemetryForTest();
    }

    @Test
    @DisplayName("payload is a DELTA non-monotonic sum named compute.invalid.byReason.schema-version")
    void emitsDeltaSumWithExactMetricName() {
        String json = new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(7);

        // Exact metric name — the O2 stream (dots -> underscores) must be
        // compute_invalid_byReason_schema-version for the alert to match.
        assertThat(json).contains("\"name\":\"compute.invalid.byReason.schema-version\"");
        assertThat(json).contains("\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\"");
        assertThat(json).contains("\"isMonotonic\":false");
        assertThat(json).contains("\"asInt\":7");
        assertThat(json).contains("\"service.name\",\"value\":{\"stringValue\":\"compute\"}");
        // DELTA + non-monotonic: a replay of legacy rows never re-fires value > 0.
        assertThat(json).doesNotContain("AGGREGATION_TEMPORALITY_CUMULATIVE");
        assertThat(json).doesNotContain("\"isMonotonic\":true");
    }

    @Test
    @DisplayName("source idle-at-tail episodes ship as their own DELTA sum (tracker 14 P7/P10)")
    void sourceIdleAtTailShipsAsDeltaSum() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        // 2-arg overload carries the drained episode count; the stream name
        // (dots -> underscores) must be compute_source_idle_at_tail for the O2
        // alert to match.
        String json = emitter.buildMetricsJson(0, 2);
        assertThat(json).contains("\"name\":\"compute.source.idle.at.tail\"");
        assertThat(json).contains("\"asInt\":2");
        assertThat(json).contains("\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\"");
        assertThat(json).contains("\"isMonotonic\":false");

        // The 1-arg convenience keeps existing call sites green and simply
        // carries a zero episode count — the metric is always present.
        String twoArg = emitter.buildMetricsJson(0);
        assertThat(twoArg).contains("\"name\":\"compute.source.idle.at.tail\"");
        assertThat(twoArg).contains("\"asInt\":0");
    }
    @Test
    @DisplayName("signal KV non-canonical filter count ships as its own DELTA sum (DEC-035)")
    void signalKvFilteredShipsAsDeltaSum() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        // 3-arg overload carries the drained count; the stream name
        // (dots -> underscores) must be compute_signal_kv_filtered_noncanonical
        // for the O2 query to match.
        String json = emitter.buildMetricsJson(0, 0, 3);
        assertThat(json).contains("\"name\":\"compute.signal.kv.filtered.noncanonical\"");
        assertThat(json).contains("\"asInt\":3");
        assertThat(json).contains("\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\"");
        assertThat(json).contains("\"isMonotonic\":false");

        // The 1- and 2-arg conveniences keep existing call sites green and
        // simply carry a zero count — the metric is always present.
        assertThat(emitter.buildMetricsJson(0))
                .contains("\"name\":\"compute.signal.kv.filtered.noncanonical\"");
        assertThat(emitter.buildMetricsJson(0, 0))
                .contains("\"name\":\"compute.signal.kv.filtered.noncanonical\"");
    }

    @Test
    @DisplayName("signal KV filter counter is drained as a delta, independent of the rejection counter")
    void signalKvFilteredCounterIsDrainedAsDelta() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        assertThat(emitter.drainSignalKvFilteredNonCanonicalDelta()).isZero();

        ComputeOtlpEmitter.recordSignalKvFilteredNonCanonical();
        ComputeOtlpEmitter.recordSignalKvFilteredNonCanonical();
        ComputeOtlpEmitter.recordSchemaVersionRejection();
        // each counter is drained independently
        assertThat(emitter.drainSignalKvFilteredNonCanonicalDelta()).isEqualTo(2);
        assertThat(emitter.drainDelta()).isEqualTo(1);
        // a drained increment never re-fires
        assertThat(emitter.drainSignalKvFilteredNonCanonicalDelta()).isZero();
    }

    @Test
    @DisplayName("counters are drained as deltas: each flush sees only increments since the previous")
    void staticCountersAreDrainedAsDelta() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        // no increments yet -> empty window emits 0 (alert stays quiet)
        assertThat(emitter.drainDelta()).isZero();
        assertThat(emitter.drainSourceIdleAtTailDelta()).isZero();

        ComputeOtlpEmitter.recordSchemaVersionRejection();
        ComputeOtlpEmitter.recordSchemaVersionRejection();
        ComputeOtlpEmitter.recordSourceIdleAtTail();
        // each counter is drained independently
        assertThat(emitter.drainDelta()).isEqualTo(2);
        assertThat(emitter.drainSourceIdleAtTailDelta()).isEqualTo(1);
        // flush 2 sees nothing new — a historical increment never re-fires
        assertThat(emitter.drainDelta()).isZero();
        assertThat(emitter.drainSourceIdleAtTailDelta()).isZero();
    }

    @Test
    @DisplayName("startup mode rides every flush as a gauge once recorded (CANDLE-KV-REPLAY-001 A3.4)")
    void startupModeShipsAsGaugeAfterRecording() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        // before the run records a mode, the gauge is absent
        assertThat(emitter.buildMetricsJson(0)).doesNotContain("compute.startup.mode");

        ComputeOtlpEmitter.recordStartupMode(0); // RESTORE
        String restoreJson = emitter.buildMetricsJson(0);
        assertThat(restoreJson).contains("\"name\":\"compute.startup.mode\"");
        assertThat(restoreJson).contains("\"gauge\":{\"dataPoints\":[{\"asInt\":0");
        assertThat(emitter.startupModeValue()).isZero();

        ComputeOtlpEmitter.recordStartupMode(1); // FULL_REPLAY
        assertThat(emitter.buildMetricsJson(0))
                .contains("\"gauge\":{\"dataPoints\":[{\"asInt\":1");
        assertThat(emitter.startupModeValue()).isEqualTo(1L);
    }

    @Test
    @DisplayName("recordStartupMode fails fast on any value other than 0 or 1")
    void startupModeRejectsInvalidValues() {
        assertThatThrownBy(() -> ComputeOtlpEmitter.recordStartupMode(2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ComputeOtlpEmitter.recordStartupMode(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── REQ-FC-006 + DEC-038: beyond-lateness drops + dedup telemetry ──────

    @Test
    @DisplayName("beyond-lateness drops ship as a DELTA sum carrying the latest drop's attributes")
    void candleLateDroppedShipsAsDeltaSumWithLatestAttributes() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        ComputeOtlpEmitter.recordCandleLateDrop(42L, 1_700_015_000L, 12_345L, "beyond-allowed-lateness");
        String json = emitter.buildMetricsJson(0, 0, 0, 2, 0);
        assertThat(json).contains("\"name\":\"compute.candles.late.dropped\"");
        assertThat(json).contains("\"asInt\":2");
        assertThat(json).contains("\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\"");
        assertThat(json).contains("\"isMonotonic\":false");
        // The single bounded attribute set (latest drop) — never per-key labels.
        assertThat(json).contains("\"key\":\"instrument_token\"");
        assertThat(json).contains("\"key\":\"window_end_ms\"");
        assertThat(json).contains("\"key\":\"lateness_ms\"");
        assertThat(json).contains("\"key\":\"reason\"");
        assertThat(json).contains("\"stringValue\":\"42\"");
        assertThat(json).contains("\"stringValue\":\"beyond-allowed-lateness\"");
    }

    @Test
    @DisplayName("REQ-FC-010: source throughput ships as a DELTA sum, watermark lag as a gauge")
    void reqFc010SourceAndWatermarkMetrics() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");

        // Source-record counter: drains as a DELTA (records per flush window).
        ComputeOtlpEmitter.recordSourceRecord();
        ComputeOtlpEmitter.recordSourceRecord();
        ComputeOtlpEmitter.recordSourceRecord();
        assertThat(emitter.drainSourceRecordsDelta()).isEqualTo(3); // drained by the flush thread
        assertThat(emitter.drainSourceRecordsDelta()).isZero();     // drained, never re-fires
        ComputeOtlpEmitter.recordSourceRecord();
        String json = emitter.buildMetricsJson(0, 0, 1, 0, 0);
        assertThat(json).contains("\"name\":\"compute.source.records\"");
        assertThat(json).contains("\"asInt\":1");
        assertThat(json).contains("\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_DELTA\"");

        // Watermark-lag gauge appears only once recorded (never an invented 0).
        assertThat(new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(0))
                .doesNotContain("compute.watermark.lag.ms");
        ComputeOtlpEmitter.recordWatermarkLagMs(1_234L);
        String withLag = new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(0);
        assertThat(withLag).contains("\"name\":\"compute.watermark.lag.ms\"");
        assertThat(withLag).contains("\"asInt\":1234");
        assertThat(withLag).contains("\"unit\":\"ms\"");
        assertThat(withLag).contains("\"gauge\":{\"dataPoints\"");
    }

    // The DEC-038 dedup cache-hit/miss/rehydration telemetry legs were retired
    // with design B (2026-08-16): the dedup set is authoritative Flink keyed
    // state — there is no cache and no store to measure.

    // ── tracker 14 P8.2: payload contract + delivery/outage semantics ─────

    @Test
    @DisplayName("every metric carries unit, temporality, resource attributes, and a timestamped data point")
    void payloadCarriesUnitsAndResourceAttributes() {
        String json = new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(7, 2);

        assertThat(json).contains("\"unit\":\"rejections\"");
        assertThat(json).contains("\"unit\":\"episodes\"");
        assertThat(json).contains("\"unit\":\"entries\"");
        assertThat(json).contains("\"unit\":\"buckets\"");
        assertThat(json).contains("\"unit\":\"bytes\"");
        // Both resource attributes ride every payload (service + instance).
        assertThat(json).contains("\"service.instance.id\",\"value\":{\"stringValue\":\"signal-job\"}");
        assertThat(json).contains("\"scope\":{\"name\":\"compute\"}");
    }

    @Test
    @DisplayName("payload parses as strict JSON: exact names, units, temporality, numeric timestamps")
    void payloadIsValidStrictJson() {
        String json = new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(7, 2);
        Map<?, ?> root = (Map<?, ?>) parseJson(json); // throws on any malformed/unescaped content

        List<?> resourceMetrics = (List<?>) root.get("resourceMetrics");
        assertThat(resourceMetrics).hasSize(1);
        Map<?, ?> rm = (Map<?, ?>) resourceMetrics.get(0);
        Map<?, ?> resource = (Map<?, ?>) rm.get("resource");
        List<?> attributes = (List<?>) resource.get("attributes");
        Map<String, String> attrs = new HashMap<>();
        for (Object a : attributes) {
            Map<?, ?> attr = (Map<?, ?>) a;
            Map<?, ?> value = (Map<?, ?>) attr.get("value");
            attrs.put((String) attr.get("key"), (String) value.get("stringValue"));
        }
        assertThat(attrs).containsEntry("service.name", "compute")
                .containsEntry("service.instance.id", "signal-job");

        List<?> scopeMetrics = (List<?>) rm.get("scopeMetrics");
        Map<?, ?> sm = (Map<?, ?>) scopeMetrics.get(0);
        assertThat(((Map<?, ?>) sm.get("scope")).get("name")).isEqualTo("compute");

        Map<String, Map<String, Object>> metrics = new LinkedHashMap<>();
        for (Object m : (List<?>) sm.get("metrics")) {
            Map<?, ?> metric = (Map<?, ?>) m;
            Map<String, Object> detail = new HashMap<>();
            detail.put("unit", metric.get("unit"));
            Object sum = metric.get("sum");
            Object gauge = metric.get("gauge");
            assertThat(sum == null ? gauge : sum).as("metric must be sum or gauge").isNotNull();
            Map<?, ?> agg = (Map<?, ?>) (sum != null ? sum : gauge);
            if (sum != null) {
                detail.put("temporality", agg.get("aggregationTemporality"));
                detail.put("isMonotonic", agg.get("isMonotonic"));
            }
            List<?> points = (List<?>) agg.get("dataPoints");
            Map<?, ?> point = (Map<?, ?>) points.get(0);
            detail.put("asInt", point.get("asInt"));
            detail.put("timeUnixNano", point.get("timeUnixNano"));
            metrics.put((String) metric.get("name"), detail);
        }

        assertThat(metrics).containsKeys(
                ComputeOtlpEmitter.SCHEMA_VERSION_REJECTED_METRIC,
                ComputeOtlpEmitter.DEDUP_STATE_COUNT_METRIC,
                ComputeOtlpEmitter.DEDUP_EXPIRY_INDEX_COUNT_METRIC,
                ComputeOtlpEmitter.DEDUP_STATE_BYTES_METRIC);
        for (Map<String, Object> detail : metrics.values()) {
            Object ts = detail.get("timeUnixNano");
            assertThat(ts).as("epoch-nano timestamp is a numeric string")
                    .isInstanceOf(String.class);
            assertThat(((String) ts).matches("\\d{16,19}"))
                    .as("numeric timestamp, 16-19 digits: %s", ts)
                    .isTrue();
            assertThat(Long.parseLong((String) ts)).isGreaterThan(0L);
        }
        Map<String, Object> rejected = metrics.get(ComputeOtlpEmitter.SCHEMA_VERSION_REJECTED_METRIC);
        assertThat(rejected.get("unit")).isEqualTo("rejections");
        assertThat(rejected.get("temporality")).isEqualTo("AGGREGATION_TEMPORALITY_DELTA");
        assertThat(rejected.get("isMonotonic")).isEqualTo(false);
        assertThat(rejected.get("asInt")).isEqualTo(7L);
    }

    @Test
    @DisplayName("configured resource attributes ride every payload (tracker 14 P8.0/831)")
    void configuredResourceAttributesAreIncluded() {
        try {
            ComputeOtlpEmitter.configureResourceAttributes(
                    "deployment.environment", "dev",
                    "host.name", "signal-host-1",
                    "deployment.version", "1.0.0",
                    "job.name", "signal-job",
                    "flink.execution.mode", "embedded");
            String json = new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(0);
            assertThat(json).contains("\"key\":\"deployment.environment\","
                    + "\"value\":{\"stringValue\":\"dev\"}");
            assertThat(json).contains("\"key\":\"host.name\","
                    + "\"value\":{\"stringValue\":\"signal-host-1\"}");
            assertThat(json).contains("\"key\":\"deployment.version\","
                    + "\"value\":{\"stringValue\":\"1.0.0\"}");
            assertThat(json).contains("\"key\":\"job.name\","
                    + "\"value\":{\"stringValue\":\"signal-job\"}");
            assertThat(json).contains("\"key\":\"flink.execution.mode\","
                    + "\"value\":{\"stringValue\":\"embedded\"}");
            // The fixed service attrs stay first and intact.
            assertThat(json).contains("\"service.name\",\"value\":{\"stringValue\":\"compute\"}");
            assertThat(json).contains("\"service.instance.id\",\"value\":{\"stringValue\":\"signal-job\"}");
            // Payload still parses as strict JSON with the extras present.
            Map<?, ?> root = (Map<?, ?>) parseJson(json);
            List<?> attributes = (List<?>) ((Map<?, ?>) ((Map<?, ?>) ((List<?>) root
                    .get("resourceMetrics")).get(0)).get("resource")).get("attributes");
            assertThat(attributes).hasSize(7); // 2 fixed + 5 configured
        } finally {
            ComputeOtlpEmitter.resetResourceAttributesForTest();
        }
    }

    @Test
    @DisplayName("resource attribute values are JSON-escaped (quote/backslash/control)")
    void resourceAttributeValuesAreJsonEscaped() {
        try {
            ComputeOtlpEmitter.configureResourceAttributes(
                    "host.name", "node\"quote\\path\nnewline");
            String json = new ComputeOtlpEmitter("localhost:4318").buildMetricsJson(0);
            assertThat(json).contains("node\\\"quote\\\\path\\nnewline");
            parseJson(json); // must still be valid JSON
        } finally {
            ComputeOtlpEmitter.resetResourceAttributesForTest();
        }
    }

    @Test
    @DisplayName("odd configure call fails fast")
    void oddConfigureCallFails() {
        assertThatThrownBy(() -> ComputeOtlpEmitter.configureResourceAttributes("key.only"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("auth failure is surfaced as the HTTP status and the emitter never sends credentials")
    void authFailureReturnsStatusWithoutSendingCredentials() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicReference<String> authHeader = new AtomicReference<>();
        AtomicReference<String> requestBody = new AtomicReference<>();
        server.createContext("/", ex -> {
            authHeader.set(ex.getRequestHeaders().getFirst("Authorization"));
            requestBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            ex.sendResponseHeaders(401, -1);
            ex.close();
        });
        server.start();
        try {
            ComputeOtlpEmitter emitter = new ComputeOtlpEmitter(
                    "127.0.0.1:" + server.getAddress().getPort());
            int code = emitter.flushOnce();
            assertThat(code).isEqualTo(401);
            assertThat(authHeader.get()).as("application must not send credentials")
                    .isNullOrEmpty();
            assertThat(requestBody.get())
                    .doesNotContain("password").doesNotContain("secret")
                    .doesNotContain("Authorization").doesNotContain("Basic ");
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("collector outage is swallowed (non-blocking) and recovery ships only the new delta")
    void outageIsNonBlockingAndRecoveryShipsOnlyNewDelta() throws Exception {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("127.0.0.1:1"); // refused port
        ComputeOtlpEmitter.recordSchemaVersionRejection();
        emitter.flush(); // must not throw, must not block on the failed export
        assertThat(emitter.drainDelta()).as("failed delta is dropped, never queued for retry")
                .isZero();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicReference<String> lastBody = new AtomicReference<>();
        server.createContext("/", ex -> {
            lastBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        server.start();
        try {
            // Recovery: same-JVM emitter path re-pointed at the live endpoint.
            ComputeOtlpEmitter recovered = new ComputeOtlpEmitter(
                    "127.0.0.1:" + server.getAddress().getPort());
            ComputeOtlpEmitter.recordSchemaVersionRejection(); // new increment after recovery
            int code = recovered.flushOnce();
            assertThat(code).isEqualTo(200);
            // Exactly the NEW delta — the failed pre-outage increment is not retried
            // (drained-once semantics: no duplicate/unbounded telemetry buffering).
            assertThat(lastBody.get()).contains("\"asInt\":1")
                    .contains("compute.invalid.byReason.schema-version")
                    .doesNotContain("\"asInt\":2");
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("alert log payload ships event, severity, detail, and resource attrs (P8.1 box 828)")
    void alertLogShipsEventSeverityAndDetail() {
        ComputeOtlpEmitter.configureResourceAttributes("deployment.environment", "dev");
        String json = ComputeOtlpEmitter.buildLogsJson("WARN", "startup-mode",
                "mode=FULL_REPLAY restore=false fullReplay=true");

        assertThat(json).contains("\"resourceLogs\"");
        assertThat(json).contains("\"severityText\":\"WARN\"");
        assertThat(json).contains("\"severityNumber\":13");
        assertThat(json).contains("\"body\":{\"stringValue\":\"mode=FULL_REPLAY restore=false fullReplay=true\"}");
        assertThat(json).contains("\"key\":\"event\",\"value\":{\"stringValue\":\"startup-mode\"}");
        assertThat(json).contains("\"key\":\"deployment.environment\",\"value\":{\"stringValue\":\"dev\"}");
        parseJson(json); // must be strict valid JSON

        // Severity mapping: INFO=9, WARN=13, ERROR=17; collector must map the
        // stream to trading_alerts via the stream-name header on the exporter.
        assertThat(ComputeOtlpEmitter.buildLogsJson("INFO", "startup-mode", "x"))
                .contains("\"severityNumber\":9");
        assertThat(ComputeOtlpEmitter.buildLogsJson("ERROR", "schema-preflight-failed", "x"))
                .contains("\"severityNumber\":17");
    }

    @Test
    @DisplayName("alert log emit never throws on a refused collector (off critical path)")
    void alertLogEmitIsBestEffort() {
        // Closed port -> ConnectionRefused; the contract is "log and swallow"
        // (a collector outage must not fail the job).
        ComputeOtlpEmitter.emitAlertLog("127.0.0.1:1", "INFO", "startup-mode", "mode=RESTORE");
        assertThat(true).isTrue(); // reached => did not throw
    }

    /**
     * Minimal strict JSON parser for the test payload: objects, arrays,
     * strings (with escapes), numbers, true/false/null. Throws on any
     * malformed input — an unterminated string, unescaped control character,
     * or trailing comma fails the test (P8.2 JSON-escaping contract).
     */
    private static Object parseJson(String s) {
        return new JsonParser(s).parse();
    }

    private static final class JsonParser {
        private final String s;
        private int i;

        JsonParser(String s) {
            this.s = s;
        }

        Object parse() {
            Object value = parseValue();
            skipWs();
            if (i != s.length()) {
                throw new IllegalArgumentException("trailing content at " + i + ": " + s.substring(i));
            }
            return value;
        }

        private Object parseValue() {
            skipWs();
            if (i >= s.length()) {
                throw new IllegalArgumentException("unexpected end of input");
            }
            char c = s.charAt(i);
            switch (c) {
                case '{':
                    return parseObject();
                case '[':
                    return parseArray();
                case '"':
                    return parseString();
                case 't':
                    expect("true");
                    return Boolean.TRUE;
                case 'f':
                    expect("false");
                    return Boolean.FALSE;
                case 'n':
                    expect("null");
                    return null;
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        return parseNumber();
                    }
                    throw new IllegalArgumentException("unexpected char '" + c + "' at " + i);
            }
        }

        private Map<String, Object> parseObject() {
            Map<String, Object> map = new LinkedHashMap<>();
            i++; // '{'
            skipWs();
            if (peek() == '}') {
                i++;
                return map;
            }
            while (true) {
                skipWs();
                if (peek() != '"') {
                    throw new IllegalArgumentException("expected string key at " + i);
                }
                String key = parseString();
                skipWs();
                if (peek() != ':') {
                    throw new IllegalArgumentException("expected ':' after key at " + i);
                }
                i++;
                map.put(key, parseValue());
                skipWs();
                char c = peek();
                if (c == ',') {
                    i++;
                    continue;
                }
                if (c == '}') {
                    i++;
                    return map;
                }
                throw new IllegalArgumentException("expected ',' or '}' at " + i);
            }
        }

        private List<Object> parseArray() {
            List<Object> list = new ArrayList<>();
            i++; // '['
            skipWs();
            if (peek() == ']') {
                i++;
                return list;
            }
            while (true) {
                list.add(parseValue());
                skipWs();
                char c = peek();
                if (c == ',') {
                    i++;
                    continue;
                }
                if (c == ']') {
                    i++;
                    return list;
                }
                throw new IllegalArgumentException("expected ',' or ']' at " + i);
            }
        }

        private String parseString() {
            i++; // opening quote
            StringBuilder sb = new StringBuilder();
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (i >= s.length()) {
                        throw new IllegalArgumentException("dangling escape at end");
                    }
                    char e = s.charAt(i++);
                    switch (e) {
                        case '"': case '\\': case '/':
                            sb.append(e);
                            break;
                        case 'b': sb.append('\b'); break;
                        case 'f': sb.append('\f'); break;
                        case 'n': sb.append('\n'); break;
                        case 'r': sb.append('\r'); break;
                        case 't': sb.append('\t'); break;
                        case 'u':
                            if (i + 4 > s.length()) {
                                throw new IllegalArgumentException("short \\u escape");
                            }
                            sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                            i += 4;
                            break;
                        default:
                            throw new IllegalArgumentException("bad escape \\" + e + " at " + (i - 1));
                    }
                } else if (c < 0x20) {
                    throw new IllegalArgumentException(
                            "unescaped control char U+" + Integer.toHexString(c) + " in string at " + (i - 1));
                } else {
                    sb.append(c);
                }
            }
            throw new IllegalArgumentException("unterminated string");
        }

        private Long parseNumber() {
            int start = i;
            if (peek() == '-') {
                i++;
            }
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                i++;
            }
            String num = s.substring(start, i);
            return Long.parseLong(num);
        }

        private void expect(String word) {
            if (!s.startsWith(word, i)) {
                throw new IllegalArgumentException("expected " + word + " at " + i);
            }
            i += word.length();
        }

        private char peek() {
            if (i >= s.length()) {
                throw new IllegalArgumentException("unexpected end of input");
            }
            return s.charAt(i);
        }

        private void skipWs() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
