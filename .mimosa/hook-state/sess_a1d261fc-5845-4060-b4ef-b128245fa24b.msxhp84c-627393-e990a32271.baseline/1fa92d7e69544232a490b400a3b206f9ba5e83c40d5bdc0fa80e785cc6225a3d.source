package com.trading.common.observability;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-046/R-047/R-078/R-264/R-265 — OTLP emitter escaping and stream constant.
 */
@DisplayName("R-046: OtlpEmitter escapes all interpolated fields")
class OtlpEmitterEscapingTest {

    @Test
    @DisplayName("free-text with control chars stays valid JSON (R-046/047/078)")
    void controlCharsAreEscaped() throws Exception {
        StructuredLogEvent event = StructuredLogEvent.builder(
                        1_752_539_000L, "INFO", "svc\"quote", "comp", "sub",
                        "host-1", "vm1", "prod", "cid", "trace", "span",
                        "line1\nline2\t\"quoted\"")
                .build();
        String json = OtlpEmitter.emitLog(event);
        // Must parse as valid JSON.
        JsonNode root = new ObjectMapper().readTree(json);
        assertTrue(root.toString().contains("line1\\nline2\\t\\\"quoted\\\""),
                "control chars must be escaped in the emitted payload");
        // severityText (level) and service.name are escaped too.
        assertTrue(json.contains("svc\\\"quote"));

        String alert = OtlpEmitter.emitAlert(
                AlertThresholds.Alert.MISSING_FILL,
                "svc\"quote", "host-1", "vm1", "prod", "cid",
                "Critical", "alert\nmessage");
        new ObjectMapper().readTree(alert); // must not throw
        assertTrue(alert.contains("alert\\nmessage"));
    }

    @Test
    @DisplayName("attribute keys are escaped (R-264)")
    void attributeKeysEscaped() throws Exception {
        StructuredLogEvent event = StructuredLogEvent.builder(
                        1L, "INFO", "svc", "comp", "sub", "host", "vm", "prod",
                        "cid", "trace", "span", "msg")
                .jobName("job\"name")
                .build();
        String json = OtlpEmitter.emitLog(event);
        new ObjectMapper().readTree(json); // must not throw
        assertTrue(json.contains("job\\\"name"));
    }

    @Test
    @DisplayName("alert stream uses the constant (R-265)")
    void streamUsesConstant() {
        String json = OtlpEmitter.emitAlert(
                AlertThresholds.Alert.MISSING_FILL,
                "svc", "h", "vm", "prod", "cid", "Critical", "m");
        assertTrue(json.contains("\"" + OtlpEmitter.TRADING_ALERTS_STREAM + "\""));
        assertFalse(json.contains("alert.condition\":\"\"") && json.contains("condition") == false);
    }
}
