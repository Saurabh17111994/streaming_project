package com.trading.ingestion.bridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;

/** Parses bridge lifecycle records while leaving tick records to the tick parser. */
public final class BridgeEventParser {
    private final ObjectMapper mapper;
    public BridgeEventParser(ObjectMapper mapper) { this.mapper = mapper; }

    /** R-214: parse from an already-parsed tree — the hot path parses each NDJSON line exactly once. */
    public Optional<BridgeEvent> parse(JsonNode node) throws Exception {
        if (node == null || !node.has("record_type")) return Optional.empty();
        String recordType = node.path("record_type").asText();
        if ("tick".equals(recordType)) return Optional.empty();
        // Any record_type other than tick/bridge_event (e.g. broker_quarantine)
        // is handled by another parser downstream — skip, never reject, so
        // the caller can fall through to parseQuarantine() (R-028).
        if (!"bridge_event".equals(recordType)) return Optional.empty();
        return Optional.of(new BridgeEvent(
                node.path("event").asText("unknown"),
                node.path("contract_version").asInt(-1),
                required(node, "slot_id"), required(node, "connection_id"),
                node.path("connection_epoch").asLong(-1), node.path("state").asText(""),
                node.path("assigned_tokens").asInt(0), node.path("acknowledged_tokens").asInt(0),
                node.path("rejected_tokens").asInt(0), node.path("reason").asText(""),
                node.path("received_ts_ms").asLong(0)));
    }

    public Optional<BridgeEvent> parse(String line) throws Exception {
        return parse(mapper.readTree(line));
    }

    /** R-214: parse from an already-parsed tree. */
    public Optional<BrokerQuarantine> parseQuarantine(JsonNode node) throws Exception {
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

    public Optional<BrokerQuarantine> parseQuarantine(String line) throws Exception {
        return parseQuarantine(mapper.readTree(line));
    }

    private static String required(JsonNode node, String field) {
        String value = node.path(field).asText("");
        if (value.isBlank()) throw new IllegalArgumentException(field + " is required");
        return value;
    }
}
