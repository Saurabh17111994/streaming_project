package com.trading.common.observability;

import java.util.Map;

/**
 * Typed emitter that serializes {@link StructuredLogEvent} and
 * {@link AlertThresholds.Alert} into OTLP/JSON logs records for the
 * OpenTelemetry Collector (docs/04_contracts/openobserve.md &sect;C, &sect;E).
 *
 * <p>Per the AI-agent rules, components emit OTLP via the collector and never
 * write observability data to local files except temporary buffering. This
 * class produces the record; forwarding to {@code otel-collector:4317} is done
 * by the runtime (outside {@code common}).
 */
public final class OtlpEmitter {

    /** OpenObserve stream that receives trading alerts. */
    public static final String TRADING_ALERTS_STREAM = "trading_alerts";

    private OtlpEmitter() {}

    /** Serialize a structured log event as an OTLP logs JSON document. */
    public static String emitLog(StructuredLogEvent event) {
        Map<String, String> attrs = event.toAttributes();
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[");
        sb.append("{\"key\":\"service.name\",\"value\":{\"stringValue\":\"").append(event.service).append("\"}}");
        sb.append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"trading.common\"},\"logRecords\":[{");
        sb.append("\"timeUnixNano\":\"").append(event.timestampMs * 1_000_000L).append("\",");
        sb.append("\"severityText\":\"").append(event.level).append("\",");
        sb.append("\"body\":{\"stringValue\":\"").append(event.message).append("\"},");
        sb.append("\"attributes\":[");
        boolean first = true;
        for (Map.Entry<String, String> e : attrs.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{\"key\":\"").append(e.getKey()).append("\",\"value\":{\"stringValue\":\"");
            sb.append(escapeJson(e.getValue())).append("\"}}");
        }
        sb.append("]}]}]}]}");
        return sb.toString();
    }

    /** Serialize an alert threshold breach as a {@code trading_alerts} OTLP record. */
    public static String emitAlert(AlertThresholds.Alert alert, String service, String host,
                                   String vmId, String environment, String correlationId,
                                   String category, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[");
        sb.append("{\"key\":\"service.name\",\"value\":{\"stringValue\":\"").append(service).append("\"}}");
        sb.append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"trading.common\"},\"logRecords\":[{");
        sb.append("\"timeUnixNano\":\"").append(System.currentTimeMillis() * 1_000_000L).append("\",");
        sb.append("\"severityText\":\"").append(category).append("\",");
        sb.append("\"body\":{\"stringValue\":\"").append(message).append("\"},");
        sb.append("\"attributes\":[");
        sb.append("{\"key\":\"stream\",\"value\":{\"stringValue\":\"trading_alerts\"}},");
        sb.append("{\"key\":\"alert.name\",\"value\":{\"stringValue\":\"").append(alert.name()).append("\"}},");
        sb.append("{\"key\":\"alert.condition\",\"value\":{\"stringValue\":\"").append(escapeJson(alert.condition)).append("\"}},");
        sb.append("{\"key\":\"alert.category\",\"value\":{\"stringValue\":\"").append(category).append("\"}},");
        sb.append("{\"key\":\"service\",\"value\":{\"stringValue\":\"").append(service).append("\"}},");
        sb.append("{\"key\":\"host\",\"value\":{\"stringValue\":\"").append(host).append("\"}},");
        sb.append("{\"key\":\"vm_id\",\"value\":{\"stringValue\":\"").append(vmId).append("\"}},");
        sb.append("{\"key\":\"environment\",\"value\":{\"stringValue\":\"").append(environment).append("\"}},");
        sb.append("{\"key\":\"correlation_id\",\"value\":{\"stringValue\":\"").append(correlationId).append("\"}}");
        sb.append("]}]}]}]}");
        return sb.toString();
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder o = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') o.append("\\\"");
            else if (c == '\\') o.append("\\\\");
            else if (c == '\n') o.append("\\n");
            else if (c == '\r') o.append("\\r");
            else o.append(c);
        }
        return o.toString();
    }
}
