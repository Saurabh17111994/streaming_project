package com.trading.compute.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * OTLP logs payload contract for the {@code trading_alerts} stream (tracker 14
 * P8.0 box 828). The METRIC half of the retired {@code ComputeOtlpEmitter} was
 * replaced by the native flink-metrics-otel reporter (CHG-023 item 1) — these
 * tests pin the one remaining hand-built payload: the synchronous alert-log
 * emit for client-side lifecycle events (schema-preflight failure, startup
 * mode) that a periodic reporter can never see.
 */
@DisplayName("ComputeAlertLogs JSON shape")
class ComputeAlertLogsTest {

    @Test
    @DisplayName("alert log payload ships event, severity, detail, and resource attrs (P8.1 box 828)")
    void alertLogShipsEventSeverityAndDetail() {
        ComputeAlertLogs.configureResourceAttributes("deployment.environment", "dev");
        String json = ComputeAlertLogs.buildLogsJson("WARN", "startup-mode",
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
        assertThat(ComputeAlertLogs.buildLogsJson("INFO", "startup-mode", "x"))
                .contains("\"severityNumber\":9");
        assertThat(ComputeAlertLogs.buildLogsJson("ERROR", "schema-preflight-failed", "x"))
                .contains("\"severityNumber\":17");
    }

    @Test
    @DisplayName("alert log emit never throws on a refused collector (off critical path)")
    void alertLogEmitIsBestEffort() {
        // Closed port -> ConnectionRefused; the contract is "log and swallow"
        // (a collector outage must not fail the job).
        ComputeAlertLogs.emitAlertLog("127.0.0.1:1", "INFO", "startup-mode", "mode=RESTORE");
        assertThat(true).isTrue(); // reached => did not throw
    }

    @Test
    @DisplayName("configured resource attributes ride the log payload (tracker 14 P8.0/831)")
    void configuredResourceAttributesAreIncluded() {
        try {
            ComputeAlertLogs.configureResourceAttributes(
                    "deployment.environment", "dev",
                    "host.name", "signal-host-1",
                    "deployment.version", "1.0.0",
                    "job.name", "signal-job",
                    "flink.execution.mode", "embedded");
            String json = ComputeAlertLogs.buildLogsJson("INFO", "startup-mode", "x");
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
                    .get("resourceLogs")).get(0)).get("resource")).get("attributes");
            assertThat(attributes).hasSize(7); // 2 fixed + 5 configured
        } finally {
            ComputeAlertLogs.resetResourceAttributesForTest();
        }
    }

    @Test
    @DisplayName("resource attribute values are JSON-escaped (quote/backslash/control)")
    void resourceAttributeValuesAreJsonEscaped() {
        try {
            ComputeAlertLogs.configureResourceAttributes(
                    "host.name", "node\"quote\\path\nnewline");
            String json = ComputeAlertLogs.buildLogsJson("INFO", "startup-mode", "x");
            assertThat(json).contains("node\\\"quote\\\\path\\nnewline");
            parseJson(json); // must still be valid JSON
        } finally {
            ComputeAlertLogs.resetResourceAttributesForTest();
        }
    }

    @Test
    @DisplayName("odd configure call fails fast")
    void oddConfigureCallFails() {
        assertThatThrownBy(() -> ComputeAlertLogs.configureResourceAttributes("key.only"))
                .isInstanceOf(IllegalArgumentException.class);
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
