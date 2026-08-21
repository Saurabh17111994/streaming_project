package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Payload-shape tests for the ingestion OTLP alert-log emitter. */
class OtlpAlertLogsTest {

    @Test
    void warn_payload_carries_signal_severity_event_and_detail() {
        String json = OtlpAlertLogs.buildLogsJson(
                "ingestion", "WARN", "SIGNAL-warn-jvm-heap-high", "container_memory_pct=90");
        assertTrue(json.contains("\"service.name\""));
        assertTrue(json.contains("\"stringValue\":\"ingestion\""));
        assertTrue(json.contains("\"severityNumber\":13"), "WARN must map to OTLP severity 13");
        assertTrue(json.contains("\"severityText\":\"WARN\""));
        assertTrue(json.contains("SIGNAL-warn-jvm-heap-high"));
        assertTrue(json.contains("container_memory_pct=90"));
    }

    @Test
    void severity_numbers_map_per_otlp_spec() {
        assertTrue(OtlpAlertLogs.buildLogsJson("i", "INFO", "e", "d").contains("\"severityNumber\":9"));
        assertTrue(OtlpAlertLogs.buildLogsJson("i", "ERROR", "e", "d").contains("\"severityNumber\":17"));
    }

    @Test
    void payload_is_wellformed_json() {
        String json = OtlpAlertLogs.buildLogsJson(
                "ingestion", "WARN", "SIGNAL-warn-jvm-heap-high", "val=a\"b");
        // balanced braces/brackets is a cheap well-formedness proxy here
        int depth = 0;
        boolean inString = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                inString = !inString;
            } else if (!inString) {
                if (c == '{' || c == '[') depth++;
                else if (c == '}' || c == ']') depth--;
            }
        }
        assertFalse(inString, "unterminated string in payload");
        assertTrue(depth == 0, "unbalanced braces/brackets (depth=" + depth + ")");
    }
}
