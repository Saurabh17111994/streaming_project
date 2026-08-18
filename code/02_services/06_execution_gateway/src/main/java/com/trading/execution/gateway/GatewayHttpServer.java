package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/** Minimal private endpoint; no Arrow route or broker client exists in this JVM. */
public final class GatewayHttpServer implements AutoCloseable {
    private final HttpServer server;
    private final GatewayProtocol protocol;
    private final GatewayConfig config;
    private final GatewayReadiness readiness;
    private final Consumer<JsonNode> eventConsumer;
    private final ObjectMapper mapper = new ObjectMapper();

    public GatewayHttpServer(GatewayConfig config, GatewayReadiness readiness,
            Consumer<JsonNode> eventConsumer) throws IOException {
        this.config = config; this.readiness = readiness; this.eventConsumer = eventConsumer;
        this.protocol = new GatewayProtocol(config.sharedSecret());
        this.server = HttpServer.create(new InetSocketAddress(config.bindHost(), config.bindPort()), 16);
        server.createContext("/healthz", this::health);
        server.createContext("/readyz", this::ready);
        server.createContext("/v1/events", this::events);
    }
    public void start() { server.start(); }
    private void health(HttpExchange x) throws IOException { reply(x, 200, "{\"healthy\":true}"); }
    private void ready(HttpExchange x) throws IOException {
        GatewayReadiness.Snapshot s = readiness.snapshot();
        reply(x, s.executionReady() ? 200 : 503, mapper.writeValueAsString(s));
    }
    private void events(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) { reply(x, 405, "method not allowed"); return; }
        String body = new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        GatewayProtocol.Verification v = protocol.verify(body, config.protocolVersion(), System.currentTimeMillis());
        if (!v.accepted()) { readiness.protocol(false, v.reason()); reply(x, 401, v.reason()); return; }
        GatewayReadiness.Snapshot ready = readiness.snapshot();
        if (!ready.healthy() || !ready.flussReady() || !ready.protocolReady() || !ready.durableWriteReady()) {
            reply(x, 503, "gateway not ready"); return;
        }
        if (eventConsumer == null) { reply(x, 503, "projection consumer unavailable"); return; }
        eventConsumer.accept(v.envelope().payload());
        reply(x, 202, "accepted");
    }
    private static void reply(HttpExchange x, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().set("Content-Type", "application/json");
        x.sendResponseHeaders(code, bytes.length);
        try (var out = x.getResponseBody()) { out.write(bytes); }
    }
    @Override public void close() { server.stop(0); }
}
