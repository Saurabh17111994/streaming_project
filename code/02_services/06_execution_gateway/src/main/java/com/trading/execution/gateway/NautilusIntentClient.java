package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;

/** Private gateway-to-Nautilus client. It has no Arrow dependency or credential. */
public final class NautilusIntentClient implements IntentSink {
    private final GatewayConfig config;
    private final ControlStateStore controls;
    private final GatewayProtocol protocol;
    private final GatewayReadiness readiness;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    public NautilusIntentClient(GatewayConfig config, ControlStateStore controls,
            GatewayReadiness readiness) {
        this.config = config; this.controls = controls; this.readiness = readiness;
        this.protocol = new GatewayProtocol(config.sharedSecret());
        this.client = HttpClient.newBuilder().connectTimeout(config.requestTimeout()).build();
    }

    @Override public Result forward(IntentRecord i) throws Exception {
        ControlStateStore.Lookup gate = controls.lookup(config.gateTable(), config.executionPartitionId());
        if (gate.status() != ControlStateStore.Status.FOUND) {
            readiness.fluss(false, gate.detail()); return Result.DEFERRED;
        }
        String state = gate.row().getString(2).toString();
        if (!"ENABLED".equals(state)) return Result.DEFERRED;
        // T5 owns the durable lease/fence extension to Execution_Gate. The
        // current v2 table has an epoch but no issued fence token; never mint
        // a process-local substitute and accidentally turn an ENABLED row into
        // an executable command.
        readiness.protocol(false, "durable fence token is not available");
        return Result.DEFERRED;
    }

    /**
     * Protocol handoff used once T5 supplies a durable fence token. Keeping
     * this separate makes the missing-fence state fail closed rather than
     * silently using a locally generated token.
     */
    Result sendWithFence(IntentRecord i, long epoch, String fenceToken) throws Exception {
        if (fenceToken == null || fenceToken.isBlank()) return Result.DEFERRED;
        ObjectNode payload = mapper.createObjectNode();
        payload.put("instruction_id", i.instructionId()); payload.put("candidate_id", i.candidateId());
        payload.put("trade_context_id", i.tradeContextId()); payload.put("instrument_token", i.instrumentToken());
        payload.put("symbol", i.symbol()); payload.put("exchange", i.exchange()); payload.put("side", i.side());
        payload.put("quantity", i.quantity()); payload.put("order_type", i.orderType());
        if (i.limitPricePaise() != null) payload.put("limit_price_paise", i.limitPricePaise());
        payload.put("product_type", i.productType()); payload.put("time_in_force", i.timeInForce());
        payload.put("request_hash", i.requestHash()); payload.put("schema_version", i.schemaVersion());
        String hash = GatewayProtocol.sha256(mapper.writeValueAsBytes(payload));
        var envelope = new GatewayProtocol.Envelope(config.protocolVersion(), "EXECUTION_INTENT",
                UUID.randomUUID().toString(), i.accountScopeId(), i.executionPartitionId(), hash, epoch,
                fenceToken, System.currentTimeMillis() + config.requestTimeout().toMillis(), payload, null);
        HttpRequest request = HttpRequest.newBuilder(URI.create(config.nautilusEndpoint()))
                .timeout(config.requestTimeout()).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(protocol.encode(envelope))).build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                readiness.protocol(true, "accepted by Nautilus"); return Result.FORWARDED;
            }
            readiness.protocol(false, "Nautilus returned HTTP " + response.statusCode());
            return Result.DEFERRED;
        } catch (Exception e) {
            readiness.protocol(false, e.getClass().getSimpleName()); return Result.DEFERRED;
        }
    }
}
