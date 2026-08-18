package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Versioned authenticated private envelope between this gateway and Nautilus. */
public final class GatewayProtocol {
    public record Envelope(String protocolVersion, String messageType, String requestId,
                           String accountScopeId, String executionPartitionId, String payloadHash,
                           long gateEpoch, String fenceToken, long deadlineEpochMs, JsonNode payload,
                           String authentication) {}

    public record Verification(boolean accepted, String reason, Envelope envelope) {}

    private static final String HMAC = "HmacSHA256";
    private final ObjectMapper mapper;
    private final String secret;

    public GatewayProtocol(String secret) {
        if (secret == null || secret.isBlank()) throw new IllegalArgumentException("secret required");
        this.secret = secret;
        this.mapper = new ObjectMapper();
    }

    public String encode(Envelope unsigned) throws Exception {
        String canonical = canonical(unsigned);
        String auth = sign(canonical);
        ObjectNode node = mapper.createObjectNode();
        node.put("protocol_version", unsigned.protocolVersion());
        node.put("message_type", unsigned.messageType());
        node.put("request_id", unsigned.requestId());
        node.put("account_scope_id", unsigned.accountScopeId());
        node.put("execution_partition_id", unsigned.executionPartitionId());
        node.put("payload_hash", unsigned.payloadHash());
        node.put("gate_epoch", unsigned.gateEpoch());
        node.put("fence_token", unsigned.fenceToken());
        node.put("deadline_epoch_ms", unsigned.deadlineEpochMs());
        node.set("payload", unsigned.payload());
        node.put("authentication", auth);
        return mapper.writeValueAsString(node);
    }

    public Verification verify(String json, String expectedVersion, long nowMs) {
        try {
            JsonNode n = mapper.readTree(json);
            Envelope e = new Envelope(text(n, "protocol_version"), text(n, "message_type"),
                    text(n, "request_id"), text(n, "account_scope_id"),
                    text(n, "execution_partition_id"), text(n, "payload_hash"),
                    n.path("gate_epoch").asLong(Long.MIN_VALUE), text(n, "fence_token"),
                    n.path("deadline_epoch_ms").asLong(Long.MIN_VALUE), n.path("payload"),
                    text(n, "authentication"));
            if (!Objects.equals(expectedVersion, e.protocolVersion())) return reject("unsupported version");
            if (e.requestId().isBlank() || e.accountScopeId().isBlank() || e.executionPartitionId().isBlank()
                    || e.payloadHash().isBlank() || e.fenceToken().isBlank()) return reject("missing identity");
            if (e.deadlineEpochMs() < nowMs) return reject("deadline expired");
            if (!MessageDigest.isEqual(e.authentication().getBytes(StandardCharsets.UTF_8),
                    sign(canonical(e)).getBytes(StandardCharsets.UTF_8))) return reject("authentication failed");
            if (!Objects.equals(e.payloadHash(), sha256(mapper.writeValueAsBytes(e.payload())))) {
                return reject("payload hash mismatch");
            }
            return new Verification(true, "accepted", e);
        } catch (Exception ex) {
            return reject("malformed envelope");
        }
    }

    private Verification reject(String reason) { return new Verification(false, reason, null); }

    private String canonical(Envelope e) throws Exception {
        return String.join("\n", e.protocolVersion(), e.messageType(), e.requestId(), e.accountScopeId(),
                e.executionPartitionId(), e.payloadHash(), Long.toString(e.gateEpoch()), e.fenceToken(),
                Long.toString(e.deadlineEpochMs()), mapper.writeValueAsString(e.payload()));
    }

    private String sign(String value) throws Exception {
        Mac mac = Mac.getInstance(HMAC);
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC));
        return HexFormat.of().formatHex(mac.doFinal(value.getBytes(StandardCharsets.UTF_8)));
    }

    public static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception e) { throw new IllegalStateException("SHA-256 unavailable", e); }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? "" : v.asText();
    }
}
