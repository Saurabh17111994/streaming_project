package com.trading.ingestion.telemetry;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ad-hoc OTLP/JSON log emitter for ingestion alerts (OTC-ALN-002 / the
 * observability dossier contract, § "Collector down must never fail the job").
 *
 * <p>Ships a {@code SIGNAL-warn-jvm-heap-high}-style event as a single OTLP log
 * record to the otel-collector's {@code /v1/logs} endpoint, tagged
 * {@code service.name=<serviceName>} (+ fixed {@code service.instance.id}).
 * Mirrors the compute module's {@code ComputeAlertLogs} shape but lives in
 * ingestion's own telemetry package because ingestion does not depend on the
 * compute module. <em>Best-effort only</em>: any network/collector failure is
 * swallowed (a WARN at DEBUG level) so telemetry can never take the data path
 * down — it is off the critical path by contract.
 */
public final class OtlpAlertLogs {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpAlertLogs.class);
    private static volatile String extraResource = ""; // flattened "k,v,k,v" extras, if any

    private OtlpAlertLogs() {}

    /** Optional static resource key/value pairs appended to every record. */
    public static void configureResourceAttributes(String... keyValuePairs) {
        if ((keyValuePairs.length & 1) != 0) {
            throw new IllegalArgumentException("keyValuePairs must be key,value pairs");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            sb.append(',').append(jsonStr(keyValuePairs[i])).append(':')
              .append(jsonStr(keyValuePairs[i + 1]));
        }
        extraResource = sb.toString();
    }

    /** Emit one alert log record; never throws (telemetry off the critical path). */
    public static void emit(String collectorHostPort, String serviceName,
                            String severity, String event, String detail) {
        String json = buildLogsJson(serviceName, severity, event, detail);
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
                LOG.warn("ingestion-otlp: alert log HTTP {} (collector={}, event={})",
                        code, collectorHostPort, event);
            }
            conn.disconnect();
        } catch (Exception e) {
            LOG.debug("ingestion-otlp: alert log emit failed (collector unreachable?): {}",
                    e.getMessage());
        }
    }

    /**
     * OTLP/JSON logs payload: one log record carrying the event name, severity,
     * and detail plus resource attributes (service.name/instance + configured
     * extras). Package-visible for the payload-shape tests.
     */
    static String buildLogsJson(String serviceName, String severity, String event, String detail) {
        long now = System.currentTimeMillis() * 1_000_000L; // epoch nanos
        int severityNumber = switch (severity) {
            case "WARN" -> 13;
            case "ERROR" -> 17;
            default -> 9; // INFO and anything else
        };
        StringBuilder sb = new StringBuilder(320);
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[")
          .append("{\"key\":\"service.name\",\"value\":{\"stringValue\":")
          .append(jsonStr(serviceName)).append("}}")
          .append(",{\"key\":\"service.instance.id\",\"value\":{\"stringValue\":\"ingestion\"}}")
          .append(extraResource)
          .append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"ingestion\"},\"logRecords\":[")
          .append("{\"timeUnixNano\":\"").append(now).append("\",")
          .append("\"severityNumber\":").append(severityNumber).append(',')
          .append("\"severityText\":").append(jsonStr(severity)).append(',')
          .append("\"body\":{\"stringValue\":").append(jsonStr(event)).append("},")
          .append("\"attributes\":[")
          .append("{\"key\":\"event\",\"value\":{\"stringValue\":").append(jsonStr(event)).append("}},")
          .append("{\"key\":\"detail\",\"value\":{\"stringValue\":").append(jsonStr(detail)).append("}}")
          .append("]}")             // close logRecord
          .append("]}}]}}");        // close scopeLogs, resourceLogs
        return sb.toString();
    }

    private static String jsonStr(String s) {
        if (s == null) {
            return "\"\"";
        }
        StringBuilder sb = new StringBuilder(s.length() + 16);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
