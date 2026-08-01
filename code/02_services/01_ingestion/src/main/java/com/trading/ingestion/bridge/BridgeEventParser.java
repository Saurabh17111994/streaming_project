package com.trading.ingestion.bridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;

/** Parses bridge lifecycle records while leaving tick records to the tick parser. */
public final class BridgeEventParser {
    private final ObjectMapper mapper;
    public BridgeEventParser(ObjectMapper mapper) { this.mapper = mapper; }

    public Optional<BridgeEvent> parse(String line) throws Exception {
        JsonNode node = mapper.readTree(line);
        if (node == null || !node.has("record_type")) return Optional.empty();
        String recordType = node.path("record_type").asText();
        if ("tick".equals(recordType)) return Optional.empty();
        if (!"bridge_event".equals(recordType)) throw new IllegalArgumentException("unknown record_type");
        return Optional.of(new BridgeEvent(
                node.path("event").asText("unknown"),
                node.path("contract_version").asInt(-1),
                required(node, "slot_id"), required(node, "connection_id"),
                node.path("connection_epoch").asLong(-1), node.path("state").asText(""),
                node.path("assigned_tokens").asInt(0), node.path("acknowledged_tokens").asInt(0),
                node.path("rejected_tokens").asInt(0), node.path("reason").asText(""),
                node.path("received_ts_ms").asLong(0)));
    }

    public Optional<BrokerQuarantine> parseQuarantine(String line) throws Exception {
        JsonNode node = mapper.readTree(line);
        if (node == null || !"broker_quarantine".equals(node.path("record_type").asText())) {
            return Optional.empty();
        }
        byte[] rawPayload = mapper.convertValue(node.path("raw_payload"), byte[].class);
        return Optional.of(new BrokerQuarantine(
                node.path("contract_version").asInt(-1),
                required(node, "slot_id"), required(node, "connection_id"),
                node.path("connection_epoch").asLong(-1), node.path("token").asLong(-1),
                required(node, "reason"), rawPayload, required(node, "payload_hash"),
                node.path("detected_ts_ms").asLong(0)));
    }

    private static String required(JsonNode node, String field) {
        String value = node.path(field).asText("");
        if (value.isBlank()) throw new IllegalArgumentException(field + " is required");
        return value;
    }
}
