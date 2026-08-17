package com.trading.compute.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Tracker 14 P8.2 box 2/3 — LIVE delivery proof through the real OTel collector
 * for the alert-log path: the {@link ComputeAlertLogs} payload (OTLP/HTTP JSON
 * logs) is accepted by the collector ({@code emitAlertLog} posts to
 * {@code /v1/logs}), which forwards it to OpenObserve with its own configured
 * credentials. The Java side proves in-JVM acceptance; the OpenObserve-side
 * landing proof (stream stats / PromQL) is captured separately with the skill
 * recipes.
 *
 * <p><b>Scope after CHG-023 item 1 (2026-08-17):</b> the METRIC delivery proof
 * moved to the native flink-metrics-otel reporter (its in-JVM acceptance is
 * the reporter's own export path; a live MiniCluster + collector probe is the
 * E2E's job). This test pins the one remaining hand-built OTLP path — the
 * synchronous alert-log emit for client-side lifecycle events.
 *
 * <p>No credentials leave the collector: the emitter sends no Authorization
 * header.
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6=true)} — the
 * collector must be up ({@code docker compose up -d otel-collector} in
 * {@code code/01_platform/01_docker}); the test skips when it is not.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P6", matches = "true")
@DisplayName("CANDLE-KV-REPLAY-001 P8.2: live OTLP alert-log delivery through the collector")
class ComputeAlertLogsLiveDeliveryTest {

    @Test
    @DisplayName("collector accepts the trading_alerts logs payload (HTTP 200)")
    void liveDeliveryThroughCollector() throws Exception {
        String host = System.getenv().getOrDefault("OTEL_COLLECTOR_HOST", "localhost:4318");
        String[] hp = host.split(":");
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 2_000);
        } catch (IOException e) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "OTel collector not reachable at "
                    + host + " — bring it up with: cd code/01_platform/01_docker && "
                    + "docker compose up -d otel-collector");
        }

        // emitAlertLog is best-effort (never throws); the live-delivery proof is
        // that the collector accepts the payload — surface the HTTP status by
        // posting through the same URL path the collector validates, via the
        // package-visible payload builder + a direct POST (the public emit path
        // swallows the status by contract).
        String json = ComputeAlertLogs.buildLogsJson("INFO", "startup-mode",
                "mode=RESTORE restore=true fullReplay=false");
        int code = postLogs(host, json);
        assertEquals(200, code,
                "collector must accept the trading_alerts payload (received=" + code + ")");
        System.out.println("P8.2[live-delivery] collector=" + host + " http=" + code
                + " event=startup-mode (trading_alerts OTLP/HTTP logs)");
    }

    private static int postLogs(String collectorHostPort, String json) throws IOException {
        java.net.URL url = java.net.URI.create("http://" + collectorHostPort + "/v1/logs").toURL();
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);
        try (java.io.OutputStream os = conn.getOutputStream()) {
            os.write(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        conn.disconnect();
        return code;
    }
}
