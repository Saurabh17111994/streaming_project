package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.GateStateStore;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

/** Minimal private endpoint; no Arrow route or broker client exists in this JVM. */
public final class GatewayHttpServer implements AutoCloseable {
    private static final String SINGLE_OPERATOR = "saurabh";
    private final HttpServer server;
    private final GatewayProtocol protocol;
    private final GatewayConfig config;
    private final GatewayReadiness readiness;
    private final Consumer<JsonNode> eventConsumer;
    private final GateStateStore gateStore;
    private final ObjectMapper mapper = new ObjectMapper();
    /** Concurrent in-flight projection applies (WP-2 MAX_PENDING_PROJECTION_RECORDS). */
    private final java.util.concurrent.atomic.AtomicInteger projectionInFlight =
            new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.Executor httpExecutor;

    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer) throws IOException {
        this(config, readiness, eventConsumer, new InMemoryGateStateStore(java.util.Set.of(SINGLE_OPERATOR)));
    }
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer, GateStateStore gateStore) throws IOException {
        this(config, readiness, eventConsumer, gateStore, null);
    }
    /**
     * Full constructor. {@code httpExecutor} {@code null} keeps the platform default
     * (serial handler dispatch); a pool is how the flood soak drives real concurrency.
     */
    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer, GateStateStore gateStore,
            java.util.concurrent.Executor httpExecutor) throws IOException {
        this.config = config; this.readiness = readiness; this.eventConsumer = eventConsumer;
        this.gateStore = java.util.Objects.requireNonNull(gateStore, "gateStore");
        this.httpExecutor = httpExecutor;
        this.protocol = new GatewayProtocol(config.sharedSecret());
        this.server = HttpServer.create(new InetSocketAddress(config.bindHost(), config.bindPort()), 16);
        if (httpExecutor != null) server.setExecutor(httpExecutor);
        server.createContext("/healthz", this::health);
        server.createContext("/readyz", this::ready);
        server.createContext("/v1/events", this::events);
        server.createContext("/control/approve", this::approve);
    }
    public void start() { server.start(); }
    private void health(HttpExchange x) throws IOException { reply(x, 200, "{\"healthy\":true}"); }
    private void ready(HttpExchange x) throws IOException {
        GatewayReadiness.Snapshot s = readiness.snapshot();
        reply(x, s.executionReady() ? 200 : 503, mapper.writeValueAsString(s));
    }
    // POST /control/approve {principal,executionPartitionId,epoch,evidenceHash}
    private void approve(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) { reply(x, 405, "{\"error\":\"method not allowed\"}"); return; }
        String body = new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        JsonNode n;
        try { n = mapper.readTree(body); } catch (Exception e) { reply(x, 400, "{\"error\":\"malformed json\"}"); return; }
        String principal = n.path("principal").asText("");
        String partitionId = n.path("executionPartitionId").asText(config.executionPartitionId());
        long epoch = n.path("epoch").isNumber() ? n.path("epoch").asLong() : Long.MIN_VALUE;
        String evidenceHash = n.path("evidenceHash").asText("");
        if (principal.isBlank() || evidenceHash.isBlank() || epoch == Long.MIN_VALUE || partitionId.isBlank()) {
            reply(x, 400, "{\"error\":\"principal, epoch, evidenceHash, executionPartitionId required\"}"); return;
        }
        long now = System.currentTimeMillis();
        GateRow cur = gateStore.read(partitionId);
        if (cur == null) { reply(x, 404, "{\"error\":\"no gate row for partition\"}"); return; }
        if (!SINGLE_OPERATOR.equals(principal)) {
            if (cur.state() != GateState.HALTED) gateStore.halt(partitionId, cur, "unauthorized approver "+principal, evidenceHash, now);
            reply(x, 403, "{\"error\":\"single-operator saurabh only\",\"outcome\":\"UNAUTHORIZED\"}"); return;
        }
        GateStateStore.ApprovalResult res = gateStore.approve(partitionId, principal, epoch, evidenceHash, now);
        switch (res.outcome()) {
            case APPLIED -> {
                GateRow approved = res.row();
                if (approved.state() == GateState.APPROVAL_PENDING && approved.approvalsComplete()) {
                    GateRow enabled = new GateRow(approved.partitionId(), approved.accountScopeId(), GateState.ENABLED,
                            approved.epoch(), "approved "+evidenceHash, evidenceHash,
                            approved.approval1(), approved.approval2(), approved.approvedEvidenceHash(),
                            approved.ownerInstanceId(), approved.fenceToken(), approved.fenceAcquiredTs(),
                            approved.leaseExpiresTs(), approved.fenceLostTs());
                    if (gateStore instanceof InMemoryGateStateStore mem) mem.install(enabled);
                    reply(x, 200, mapper.writeValueAsString(Map.of("status","ENABLED","epoch",enabled.epoch(),"outcome","APPLIED")));
                } else {
                    reply(x, 200, mapper.writeValueAsString(Map.of("status",approved.state().name(),"epoch",approved.epoch(),"outcome","APPLIED")));
                }
            }
            case ALREADY_APPLIED -> reply(x, 200, mapper.writeValueAsString(Map.of("status",res.row().state().name(),"epoch",res.row().epoch(),"outcome","ALREADY_APPLIED")));
            case EPOCH_MISMATCH -> {
                if (cur.state() != GateState.HALTED) gateStore.halt(partitionId, cur, "epoch mismatch approve "+epoch+" != "+cur.epoch(), evidenceHash, now);
                reply(x, 409, "{\"error\":\"epoch mismatch\",\"outcome\":\"EPOCH_MISMATCH\"}");
            }
            case UNAUTHORIZED -> {
                if (cur.state() != GateState.HALTED) gateStore.halt(partitionId, cur, "unauthorized "+principal, evidenceHash, now);
                reply(x, 403, "{\"error\":\"unauthorized\",\"outcome\":\"UNAUTHORIZED\"}");
            }
            case SAME_PRINCIPAL -> reply(x, 409, "{\"error\":\"same principal already approved\",\"outcome\":\"SAME_PRINCIPAL\"}");
            case NOT_FOUND -> reply(x, 404, "{\"error\":\"no gate row\",\"outcome\":\"NOT_FOUND\"}");
            default -> reply(x, 409, mapper.writeValueAsString(Map.of("error",res.reason()==null?"rejected":res.reason(),"outcome",res.outcome().name())));
        }
    }
    private void events(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) { reply(x, 405, "method not allowed"); return; }
        // Fail-closed: when execution is disabled the bridge is disabled and the gateway remains HALTED.
        // Offline, no FLUSS_BOOTSTRAP / Arrow deps are required to evaluate this gate.
        if (!config.executionEnabled()) { reply(x, 503, "execution disabled via EXECUTION_ENABLED"); return; }
        String body = new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        GatewayProtocol.Verification v = protocol.verify(body, config.protocolVersion(), System.currentTimeMillis());
        if (!v.accepted()) { readiness.protocol(false, v.reason()); reply(x, 401, v.reason()); return; }
        GatewayReadiness.Snapshot ready = readiness.snapshot();
        if (!ready.healthy() || !ready.flussReady() || !ready.protocolReady() || !ready.durableWriteReady()) {
            reply(x, 503, "gateway not ready"); return;
        }
        if (eventConsumer == null) { reply(x, 503, "projection consumer unavailable"); return; }
        // WP-2 backpressure bound: concurrent in-flight applies beyond
        // MAX_PENDING_PROJECTION_RECORDS flip durableWrites readiness false and
        // shed load (503) instead of queueing without bound.
        int inFlightNow = projectionInFlight.incrementAndGet();
        int maxPending = config.maxPendingProjectionRecords();
        try {
            if (inFlightNow > maxPending) {
                readiness.durableWrites(false,
                        "projection backlog " + inFlightNow + " > " + maxPending);
                reply(x, 503, "projection backlog exceeds MAX_PENDING_PROJECTION_RECORDS");
                return;
            }
            eventConsumer.accept(v.envelope().payload());
            reply(x, 202, "accepted");
        } finally {
            // Restore only on FULL drain: while any apply is in flight past a
            // flagged bound the gateway stays conservatively not-ready.
            if (projectionInFlight.decrementAndGet() == 0
                    && !readiness.snapshot().durableWriteReady()) {
                readiness.durableWrites(true, "projection backlog drained");
            }
        }
    }
    private static void reply(HttpExchange x, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().set("Content-Type", "application/json");
        x.sendResponseHeaders(code, bytes.length);
        try (var out = x.getResponseBody()) { out.write(bytes); }
    }
    @Override public void close() { server.stop(0); }
}
