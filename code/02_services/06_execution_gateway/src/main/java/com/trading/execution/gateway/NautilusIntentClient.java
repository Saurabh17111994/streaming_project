package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.apache.fluss.row.InternalRow;

/**
 * Private gateway-to-Nautilus client. It has no Arrow dependency or credential.
 *
 * <p>Tier 0 T5 — durable fence wiring: {@link #forward(IntentRecord)} reads the
 * durable {@code Execution_Gate} row via {@link ControlStateStore#lookup(String, Object...)}
 * (i.e. {@code config.gateTable()} / {@code config.executionPartitionId()}), decodes
 * it with the {@link ExecutionGateColumns} v3 layout (same mapping as
 * {@link com.trading.common.schema.execution.FlussGateStateStore#fromRow}), and
 * only forwards when the gate is {@code ENABLED} with a live fence token.
 * Fail-closed: never mints a process-local fence token; an {@code ENABLED} row
 * without a valid, unexpired {@code fenceToken}/{@code leaseExpiresTs} is treated
 * as not executable and returns {@link IntentSink.Result#DEFERRED}.
 */
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

    /**
     * Forwards an intent to Nautilus only when the durable gate authorizes it.
     *
     * <ol>
     *   <li>Looks up the durable gate row via
     *       {@code controls.lookup(config.gateTable(), config.executionPartitionId())}.</li>
     *   <li>If lookup status is not {@code FOUND}, marks Fluss not-ready
     *       ({@code readiness.fluss(false, detail)}) and returns {@code DEFERRED}
     *       — fail-closed on missing/unavailable control state.</li>
     *   <li>Decodes the {@link org.apache.fluss.row.InternalRow} using the
     *       {@link ExecutionGateColumns} v3 layout (mirrors
     *       {@code FlussGateStateStore.fromRow}). If {@code state != ENABLED},
     *       returns {@code DEFERRED} without touching the fence.</li>
     *   <li>Extracts {@code epoch} and {@code fenceToken}. Validates
     *       {@code fenceToken != null/blank} (treats absent or {@code 0} as unavailable
     *       — a never-acquired fence) and that the lease has not expired:
     *       when {@code leaseExpiresTs != null}, requires
     *       {@code System.currentTimeMillis() <= leaseExpiresTs}. On any fence
     *       invalidity, marks {@code readiness.protocol(false,
     *       "fence token unavailable or expired")} and returns {@code DEFERRED}.</li>
     *   <li>Otherwise delegates to {@link #sendWithFence(IntentRecord, long, String)}
     *       with the durable {@code epoch} and {@code String.valueOf(fenceToken)}.</li>
     * </ol>
     *
     * <p>Never mints a local token: the only fence that can authorize a bridge
     * command is the one already persisted in the durable gate row.
     */
    @Override public Result forward(IntentRecord i) throws Exception {
        ControlStateStore.Lookup gate = controls.lookup(config.gateTable(), config.executionPartitionId());
        if (gate.status() != ControlStateStore.Status.FOUND) {
            readiness.fluss(false, gate.detail());
            return Result.DEFERRED;
        }
        InternalRow row = gate.row();
        if (row == null) {
            readiness.fluss(false, "gate row is null");
            return Result.DEFERRED;
        }

        // Decode state — Execution_Gate v3 column 2. Mirrors FlussGateStateStore.fromRow.
        String state;
        try {
            if (row.isNullAt(ExecutionGateColumns.STATE)) {
                return Result.DEFERRED;
            }
            state = row.getString(ExecutionGateColumns.STATE).toString();
        } catch (Exception e) {
            return Result.DEFERRED;
        }
        if (!"ENABLED".equals(state)) {
            return Result.DEFERRED;
        }

        // Extract epoch (column 3, BIGINT NOT NULL in DDL)
        long epoch;
        try {
            epoch = row.getLong(ExecutionGateColumns.EPOCH);
        } catch (Exception e) {
            readiness.protocol(false, "fence token unavailable or expired");
            return Result.DEFERRED;
        }

        // Extract and validate durable fenceToken (column 11, BIGINT nullable).
        // GateRow stores fenceToken as long; 0 means never acquired — fail closed.
        String fenceToken;
        try {
            if (row.isNullAt(ExecutionGateColumns.FENCE_TOKEN)) {
                readiness.protocol(false, "fence token unavailable or expired");
                return Result.DEFERRED;
            }
            long ft = row.getLong(ExecutionGateColumns.FENCE_TOKEN);
            if (ft == 0L) {
                readiness.protocol(false, "fence token unavailable or expired");
                return Result.DEFERRED;
            }
            fenceToken = String.valueOf(ft);
            if (fenceToken.isBlank()) {
                readiness.protocol(false, "fence token unavailable or expired");
                return Result.DEFERRED;
            }
        } catch (Exception e) {
            readiness.protocol(false, "fence token unavailable or expired");
            return Result.DEFERRED;
        }

        // Validate lease has not expired (column 13, BIGINT nullable). If leaseExpiresTs
        // is present, the fence is only live while System.currentTimeMillis() <= leaseExpiresTs.
        try {
            if (!row.isNullAt(ExecutionGateColumns.LEASE_EXPIRES_TS)) {
                long leaseExpiresTs = row.getLong(ExecutionGateColumns.LEASE_EXPIRES_TS);
                if (System.currentTimeMillis() > leaseExpiresTs) {
                    readiness.protocol(false, "fence token unavailable or expired");
                    return Result.DEFERRED;
                }
            }
        } catch (Exception e) {
            readiness.protocol(false, "fence token unavailable or expired");
            return Result.DEFERRED;
        }

        return sendWithFence(i, epoch, fenceToken);
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
