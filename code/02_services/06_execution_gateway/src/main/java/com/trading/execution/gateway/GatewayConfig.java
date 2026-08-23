package com.trading.execution.gateway;

import java.time.Duration;
import java.util.Map;

/** Strict, private-only configuration for the Fluss execution gateway. */
public record GatewayConfig(
        String flussBootstrap,
        String flussDatabase,
        String intentTable,
        String gateTable,
        String attemptsTable,
        String correlationTable,
        String ledgerTable,
        String haltTable,
        String bindHost,
        int bindPort,
        String nautilusEndpoint,
        String protocolVersion,
        String sharedSecret,
        Duration requestTimeout,
        Duration pollTimeout,
        String accountScopeId,
        String executionPartitionId) {

    public GatewayConfig {
        require(flussBootstrap, "FLUSS_BOOTSTRAP");
        require(flussDatabase, "FLUSS_DATABASE");
        require(intentTable, "EXECUTION_INTENT_TABLE");
        require(gateTable, "EXECUTION_GATE_TABLE");
        require(attemptsTable, "EXECUTION_ATTEMPTS_TABLE");
        require(correlationTable, "ORDER_CORRELATION_TABLE");
        require(ledgerTable, "PROJECTION_LEDGER_TABLE");
        require(haltTable, "SAFETY_HALT_TABLE");
        require(bindHost, "GATEWAY_BIND_HOST");
        require(nautilusEndpoint, "NAUTILUS_PRIVATE_ENDPOINT");
        require(protocolVersion, "GATEWAY_PROTOCOL_VERSION");
        require(sharedSecret, "GATEWAY_SHARED_SECRET");
        require(accountScopeId, "ACCOUNT_SCOPE_ID");
        require(executionPartitionId, "EXECUTION_PARTITION_ID");
        if (bindPort < 0 || bindPort > 65535) throw new IllegalArgumentException("invalid bind port");
        if (requestTimeout.isZero() || requestTimeout.isNegative()
                || pollTimeout.isZero() || pollTimeout.isNegative()) {
            throw new IllegalArgumentException("timeouts must be positive");
        }
    }

    public static GatewayConfig fromEnvironment() {
        return from(Map.ofEntries(
                Map.entry("FLUSS_BOOTSTRAP", env("FLUSS_BOOTSTRAP", "localhost:9123")),
                Map.entry("FLUSS_DATABASE", env("FLUSS_DATABASE", "default")),
                Map.entry("EXECUTION_INTENT_TABLE", env("EXECUTION_INTENT_TABLE", "Execution_Intent")),
                Map.entry("EXECUTION_GATE_TABLE", env("EXECUTION_GATE_TABLE", "Execution_Gate")),
                Map.entry("EXECUTION_ATTEMPTS_TABLE", env("EXECUTION_ATTEMPTS_TABLE", "Execution_Attempts")),
                Map.entry("ORDER_CORRELATION_TABLE", env("ORDER_CORRELATION_TABLE", "Order_Correlation")),
                Map.entry("PROJECTION_LEDGER_TABLE", env("PROJECTION_LEDGER_TABLE", "Postback_Projection_Ledger")),
                Map.entry("SAFETY_HALT_TABLE", env("SAFETY_HALT_TABLE", "Safety_Halt_Requests")),
                Map.entry("GATEWAY_BIND_HOST", env("GATEWAY_BIND_HOST", "127.0.0.1")),
                Map.entry("GATEWAY_BIND_PORT", env("GATEWAY_BIND_PORT", "9180")),
                Map.entry("NAUTILUS_PRIVATE_ENDPOINT", env("NAUTILUS_PRIVATE_ENDPOINT", "http://127.0.0.1:9190/v1/intents")),
                Map.entry("GATEWAY_PROTOCOL_VERSION", env("GATEWAY_PROTOCOL_VERSION", "execution-gateway.v1")),
                Map.entry("GATEWAY_SHARED_SECRET", requiredEnv("GATEWAY_SHARED_SECRET")),
                Map.entry("GATEWAY_REQUEST_TIMEOUT_MS", env("GATEWAY_REQUEST_TIMEOUT_MS", "2000")),
                Map.entry("GATEWAY_POLL_TIMEOUT_MS", env("GATEWAY_POLL_TIMEOUT_MS", "250")),
                Map.entry("ACCOUNT_SCOPE_ID", requiredEnv("ACCOUNT_SCOPE_ID")),
                Map.entry("EXECUTION_PARTITION_ID", requiredEnv("EXECUTION_PARTITION_ID"))));
    }

    static GatewayConfig from(Map<String, String> e) {
        return new GatewayConfig(
                e.get("FLUSS_BOOTSTRAP"), e.get("FLUSS_DATABASE"), e.get("EXECUTION_INTENT_TABLE"),
                e.get("EXECUTION_GATE_TABLE"), e.get("EXECUTION_ATTEMPTS_TABLE"),
                e.get("ORDER_CORRELATION_TABLE"), e.get("PROJECTION_LEDGER_TABLE"), e.get("SAFETY_HALT_TABLE"),
                e.get("GATEWAY_BIND_HOST"), integer(e, "GATEWAY_BIND_PORT"), e.get("NAUTILUS_PRIVATE_ENDPOINT"),
                e.get("GATEWAY_PROTOCOL_VERSION"),
                e.get("GATEWAY_SHARED_SECRET"), Duration.ofMillis(longValue(e, "GATEWAY_REQUEST_TIMEOUT_MS")),
                Duration.ofMillis(longValue(e, "GATEWAY_POLL_TIMEOUT_MS")), e.get("ACCOUNT_SCOPE_ID"),
                e.get("EXECUTION_PARTITION_ID"));
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String requiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(key + " is required");
        return value;
    }

    private static int integer(Map<String, String> e, String key) {
        try { return Integer.parseInt(e.get(key)); }
        catch (Exception ex) { throw new IllegalArgumentException(key + " must be an integer", ex); }
    }

    private static long longValue(Map<String, String> e, String key) {
        try { return Long.parseLong(e.get(key)); }
        catch (Exception ex) { throw new IllegalArgumentException(key + " must be an integer", ex); }
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
    }
}
