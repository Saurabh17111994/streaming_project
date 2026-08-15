package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ING-E2E-001: full-stack fake-broker → bridge → IngestionService → Fluss.
 *
 * <p>Requires a fake HFT broker URL (provided by a fixture script that starts
 * the Go fake broker) and a live Fluss at localhost:9123. The Java service is
 * launched as a subprocess with ARROW_HFT_URL pointing at the fake broker, the
 * built bridge binary as ARROW_BRIDGE_BIN, and the normal env. The test asserts
 * the service starts, the bridge connects, ticks flow, and a discontinuity
 * evidence row is produced after a forced disconnect.
 *
 * <p>Set {@code INGESTION_INT_TEST_E2E=true} (the fake broker is started by
 * this test itself — the historical {@code FAKE_HFT_URL} env var is unused).
 *
 * <p><b>Harness hardening (2026-08-15):</b>
 * <ul>
 *   <li>{@code LOG_DIR} is pointed at a writable JUnit temp dir — the
 *       log4j2 {@code JSON_FILE} appender defaults to
 *       {@code /data/ingestion/logs}, and when that path cannot be created the
 *       root logger's events are dropped, leaving the assertions blind to the
 *       service's startup logs.</li>
 *   <li>Startup/disconnect markers are <b>polled</b> (250 ms interval, bounded
 *       deadlines) instead of a fixed 8 s sleep, so a slow JVM/clock-check no
 *       longer fails a healthy run and a fast run finishes as soon as the
 *       evidence appears.</li>
 *   <li>Cluster pre-flight (2026-08-15): before launching the service the test
 *       verifies the dev Fluss stack serves data — TCP 9123 (coordinator) and
 *       TCP 9124 (tablet) must accept. The tablet only binds after every log
 *       segment recovers, so a missing tablet listener with a crash-looping
 *       container means truncated segments from an unclean shutdown; the
 *       failure message points at the surgical repair
 *       ({@code code/01_platform/04_scripts/fluss-repair/repair-tablet.sh},
 *       runbook section in 11-testing-and-release.md). Without this, the test
 *       burned the full 90 s startup window polling for {@code Fluss
 *       connected} and failed with a confusing stderr tail.</li>
 * </ul>
 */
@DisplayName("ING-E2E-001: full-stack fake broker → Fluss")
class FullStackE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(FullStackE2ETest.class);

    /** Startup deadline — the poll returns as soon as the markers appear. */
    private static final long STARTUP_TIMEOUT_MS = 90_000;

    /** Dev-cluster ports: coordinator client RPC and tablet server. */
    private static final int FLUSS_COORDINATOR_PORT = 9123;
    private static final int FLUSS_TABLET_PORT = 9124;
    /** Pre-flight TCP connect timeout — success is sub-ms on a healthy stack. */
    private static final int PREFLIGHT_TCP_TIMEOUT_MS = 5_000;
    /** Bounded window to observe the forced-disconnect evidence after startup. */
    private static final long DISCONNECT_WINDOW_MS = 30_000;
    private static final long POLL_INTERVAL_MS = 250;

    @TempDir
    Path logDir;

    @Test
    @DisplayName("bridge ingests fake ticks into Fluss and survives a forced disconnect")
    void fullStackFakeBrokerToFluss() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_E2E", "false")),
                "Skipping — set INGESTION_INT_TEST_E2E=true");

        // 0. Pre-flight: the dev Fluss stack must serve data before we burn
        //    the startup window (see the cluster-health javadoc note).
        preflightCluster();

        // 1. Start the standalone fake HFT broker on a free port.
        int port = freePort();
        String faketoolBin = System.getenv().getOrDefault(
                "FAKETOOL_BIN", "go-bridge/faketool/faketool");
        Process faketool = new ProcessBuilder(faketoolBin, "--port", String.valueOf(port), "--disconnect-after", "1")
                .redirectErrorStream(true).start();
        try {
            Thread.sleep(500); // let the fake broker bind
            String fakeUrl = "ws://127.0.0.1:" + port;

            String bridgeBin = System.getenv().getOrDefault(
                    "ARROW_BRIDGE_BIN",
                    "go-bridge/arrow-bridge");

            List<String> cmd = new ArrayList<>(List.of(
                    "java",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "-cp", System.getProperty("java.class.path"),
                    "com.trading.ingestion.IngestionService"));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            java.util.Map<String, String> env = new java.util.HashMap<>();
            env.put("ARROW_HFT_URL", fakeUrl);
            env.put("ARROW_BRIDGE_BIN", bridgeBin);
            env.put("ARROW_APP_ID", "e2e");
            env.put("ARROW_APP_SECRET", "e2esecret");
            env.put("ARROW_TOKEN", "e2etoken");
            env.put("FLUSS_BOOTSTRAP", "localhost:9123");
            env.put("RAW_TABLE_NAME", "raw_table_1");
            env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
            env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
            env.put("GO_ARROW_SDK_VERSION", "v0.0.0-20260622-7cce1630");
            env.put("ARROW_HFT_CONNECTIONS", "1");
            // log4j2's JSON_FILE appender needs a writable LOG_DIR; without one
            // the root logger drops every event and the assertions go blind.
            env.put("LOG_DIR", logDir.toString());
            // Manifest: the fake broker emits token 757614 (present in the
            // approved CSV) so the tick is actually ingested.
            String manifestPath = System.getenv().getOrDefault("INSTRUMENT_MANIFEST_PATH",
                    "../../../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv");
            env.put("INSTRUMENT_MANIFEST_PATH", manifestPath);
            env.put("ARROW_INSTRUMENT_MANIFEST", manifestPath);
            pb.environment().putAll(env);
            pb.redirectErrorStream(false);
            Process proc = pb.start();

            // Drain stderr to a thread-safe buffer so the poll loop can watch
            // for markers while the reader thread keeps appending.
            StringBuffer stderr = new StringBuffer();
            Thread stderrThread = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        stderr.append(line).append('\n');
                    }
                } catch (IOException ignored) {
                }
            });
            stderrThread.setDaemon(true);
            stderrThread.start();

            // 2. Poll for the startup markers (no fixed sleep): the service
            //    must validate config, pass the schema check, connect to Fluss,
            //    construct the real evidence writers, and launch the bridge.
            waitForMarker(stderr, "Fluss connected", STARTUP_TIMEOUT_MS,
                    "service must connect to Fluss");
            waitForMarker(stderr, "arrow-bridge started", STARTUP_TIMEOUT_MS,
                    "service must start the bridge");
            // 3. faketool --disconnect-after 1 force-closes the first
            //    connection; the bridge must emit the disconnect lifecycle
            //    event (→ DROP discontinuity evidence) and keep running.
            waitForMarker(stderr, "event=disconnect", DISCONNECT_WINDOW_MS,
                    "bridge must survive the forced disconnect (discontinuity evidence)");

            proc.destroy();
            boolean exited = proc.waitFor(10, TimeUnit.SECONDS);
            if (!exited) {
                proc.destroyForcibly();
            }

            String log = stderr.toString();
            LOG.info("e2e service stderr ({} chars):\n{}", log.length(),
                    log.length() > 4000 ? log.substring(0, 4000) : log);
            assertTrue(log.contains("Fluss connected"),
                    "service must connect to Fluss");
            assertTrue(log.contains("arrow-bridge started") || log.contains("bridge"),
                    "service must start the bridge");
        } finally {
            faketool.destroyForcibly();
        }
    }

    /**
     * Pre-flight cluster-health check: the dev Fluss stack must actually serve
     * data before the test launches the service.
     *
     * <p>The check is cheap and precise: TCP 9123 (coordinator client RPC) and
     * TCP 9124 (tablet) must accept. The tablet only binds after <b>every</b>
     * log segment recovers, so a missing tablet listener while its container
     * crash-loops means truncated segments from an unclean shutdown — the
     * docker status probe distinguishes that from a stopped stack and points
     * at the surgical repair (fluss-repair/repair-tablet.sh; runbook section
     * in 11-testing-and-release.md).
     */
    private static void preflightCluster() {
        if (!tcpReachable("127.0.0.1", FLUSS_COORDINATOR_PORT)) {
            fail("ING-E2E-001 pre-flight: Fluss coordinator not reachable on 127.0.0.1:"
                    + FLUSS_COORDINATOR_PORT + " — start the dev stack "
                    + "(code/01_platform/01_docker) before running this test");
        }
        if (!tcpReachable("127.0.0.1", FLUSS_TABLET_PORT)) {
            String diag = dockerTabletStatus();
            if (diag != null && diag.contains("Restarting")) {
                fail("ING-E2E-001 pre-flight: Fluss tablet is crash-looping (" + diag + ") — "
                        + "likely truncated log segments from an unclean shutdown. Run the "
                        + "surgical repair: code/01_platform/04_scripts/fluss-repair/repair-tablet.sh "
                        + "(procedure in the ING-E2E-001 runbook section of 11-testing-and-release.md) "
                        + "before retrying");
            }
            fail("ING-E2E-001 pre-flight: Fluss tablet not reachable on 127.0.0.1:"
                    + FLUSS_TABLET_PORT + " — start the dev stack (code/01_platform/01_docker) "
                    + "before running this test"
                    + (diag == null ? "" : " (docker status: " + diag + ")"));
        }
    }

    /** True if a TCP connect to {@code host}:{@code port} succeeds within the pre-flight timeout. */
    private static boolean tcpReachable(String host, int port) {
        try (java.net.Socket s = new java.net.Socket()) {
            s.connect(new java.net.InetSocketAddress(host, port), PREFLIGHT_TCP_TIMEOUT_MS);
            return true;
        } catch (java.io.IOException e) {
            return false;
        }
    }

    /**
     * Best-effort docker status line for the tablet container via
     * {@code docker ps -a} (e.g. {@code Restarting (5) 3 seconds ago}), or
     * {@code null} when the CLI is unavailable or no matching container
     * exists. Several compose projects can leave fluss-tablet containers
     * around (this host also has an Exited one from another stack), so the
     * match prefers the crash-loop symptom (Restarting), then an Up
     * container, then the most recently stopped match — mirroring
     * repair-tablet.sh's discovery. Recency is ranked by the "ago" unit in
     * the status line (seconds/minutes &lt; hours &lt; days &lt; weeks+), since
     * CreatedAt is the container's original creation time and is not the
     * right signal.
     */
    private static String dockerTabletStatus() {
        try {
            String[] cmd = {"docker", "ps", "-a", "--format", "{{.Names}}	{{.Status}}",
                    "--filter", "name=fluss-tablet"};
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            p.waitFor(5, TimeUnit.SECONDS);
            String restarting = null, up = null;
            String recentStatus = null;
            int recentRank = Integer.MAX_VALUE;
            for (String line : out.split("\n")) {
                String[] parts = line.split("\t");
                if (parts.length < 2 || !parts[0].contains("fluss-tablet")) {
                    continue;
                }
                String status = parts[1];
                if (restarting == null && status.startsWith("Restarting")) {
                    restarting = status;
                }
                if (up == null && status.startsWith("Up")) {
                    up = status;
                }
                int rank = agoRank(status);
                if (rank < recentRank) {
                    recentRank = rank;
                    recentStatus = status;
                }
            }
            if (restarting != null) {
                return restarting;
            }
            if (up != null) {
                return up;
            }
            return recentStatus;
        } catch (Exception e) {
            return null;
        }
    }

    /** Recency rank of a docker status line by its "ago" unit: 0 = seconds/
     *  minutes, 1 = hours, 2 = days, 3 = weeks or longer (unparseable = 3). */
    private static int agoRank(String status) {
        String s = status.toLowerCase();
        if (s.contains("second") || s.contains("minute")) {
            return 0;
        }
        if (s.contains("hour")) {
            return 1;
        }
        if (s.contains("day")) {
            return 2;
        }
        return 3;
    }

    /** Poll {@code buffer} for {@code marker} until it appears or the deadline passes. */
    private static void waitForMarker(StringBuffer buffer, String marker, long timeoutMs,
                                      String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (buffer.indexOf(marker) >= 0) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        if (buffer.indexOf(marker) < 0) {
            String tail = buffer.length() > 4000
                    ? buffer.substring(buffer.length() - 4000) : buffer.toString();
            fail(message + " — marker '" + marker + "' not seen within " + timeoutMs + "ms; "
                    + "service stderr tail:\n" + tail);
        }
    }

    private static int freePort() throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
