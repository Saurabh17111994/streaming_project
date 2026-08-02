package com.trading.ingestion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.bridge.BridgeEvent;
import com.trading.ingestion.bridge.BridgeEventParser;
import com.trading.ingestion.bridge.BrokerQuarantine;
import com.trading.ingestion.bridge.PayloadHashValidator;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.fingerprint.FingerprintBuilder;
import com.trading.ingestion.health.HealthProbe;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.health.ReadinessFile;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.model.ValidityClassification;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.shutdown.UncertaintyJournal;
import com.trading.ingestion.telemetry.OtlpMetricsEmitter;
import com.trading.ingestion.write.AppendTracker;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingestion main entry point — consumes NDJSON ticks from the Arrow Go bridge
 * (stdin), normalizes, fingerprints, and appends to Fluss {@code raw_table_1}.
 *
 * <h3>Pipeline</h3>
 * <pre>{@code
 * Go arrow-bridge stdout (NDJSON)
 *   → IngestionService.main() reads stdin line-at-a-time
 *   → parse Tick JSON → RawTick → valid? → TickPacket
 *   → FingerprintBuilder.build(…) → fingerprint
 *   → RawTickWriter.write(…) → Fluss append
 * }</pre>
 *
 * <p>The Go arrow-bridge process uses the official Arrow Go SDK for:
 * authentication (AutoLogin or static token), WebSocket connect, binary
 * frame decode (all 4 modes — LTP/LTPC/QUOTE/FULL including 5-level depth),
 * zstd decompression (HFT), subscription management, keepalive, and
 * reconnection. Java owns only the platform path: normalize, fingerprint,
 * Fluss append, backpressure, and observability.
 *
 * <p>See {@code docs/04_contracts/01-ingestion.md},
 * {@code docs/08_implementation/03-ingestion.md}.
 */
public final class IngestionService {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionService.class);

    private static final String VERSION = "0.2.0";
    private static final String FINGERPRINT_ALGO = "SHA-256";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    private static final long FRAME_STALE_MS = 15_000L;
    private static final long SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS = 30_000L;
    private static final double SLOW_FLUSS_PAUSE_PERCENT = 0.90;
    private static final double SLOW_FLUSS_RESUME_PERCENT = 0.50;
    /** Bridge restarts after an unexpected process exit (plan: restart exactly once). */
    private static final int MAX_BRIDGE_RESTARTS = 1;
    /** Wait before restarting a crashed bridge (plan: wait 1 second). */
    private static final long BRIDGE_RESTART_WAIT_MS = 1_000L;

    private final String instanceId;
    private final AppendTracker tracker;
    private final HealthProbe health;
    private final RawTickWriter writer;
    private final Map<Long, Instrument> instrumentMap;
    private final AtomicLong frameCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);
    private volatile boolean running;
    private volatile boolean subscriptionPaused;
    private volatile long lastFrameNanos;
    private volatile long lastResourceRefreshNanos;

    /** Watchdog for broker-staleness detection during feed outages (R-108). */
    private final java.util.concurrent.ScheduledExecutorService stalenessWatchdog =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ingestion-staleness-watchdog");
                t.setDaemon(true);
                return t;
            });

    private final IngestionConfig config;
    private final NtpClockChecker clock;
    private final UncertaintyJournal journal;
    private final OtlpMetricsEmitter metrics;
    private final QuarantineWriter quarantineWriter;
    private final DiscontinuityWriter discontinuityWriter;
    private final com.trading.ingestion.safety.SafetyHaltWriter safetyHaltWriter;
    private final String manifestFingerprint;
    private final String assignedTokenSetHash;
    private final java.util.Set<String> safetyEmitted = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final ReadinessFile readinessFile;
    private final BridgeEventParser bridgeEventParser = new BridgeEventParser(MAPPER);
    private final AtomicLong connectionEpoch = new AtomicLong(0);
    private volatile DiscontinuityWriter.LastTickSnapshot lastTickSnapshot;

    public IngestionService(String instanceId,
                             List<Instrument> instruments,
                             FlussRowConverter flussWriter,
                             IngestionConfig config,
                             NtpClockChecker clock) {
        this.config = config;
        this.clock = clock;
        if (config.uncertaintyJournalPath != null && !config.uncertaintyJournalPath.isBlank()) {
            this.journal = new UncertaintyJournal(java.nio.file.Paths.get(config.uncertaintyJournalPath));
        } else {
            this.journal = new UncertaintyJournal();
        }
        this.instanceId = instanceId;
        this.instrumentMap = new ConcurrentHashMap<>();
        for (Instrument inst : instruments) {
            instrumentMap.put(inst.instrumentToken(), inst);
        }
        this.tracker = new AppendTracker(config.maxPendingRecords, config.maxPendingBytes,
                config.pendingWarningPercent);
        this.health = new HealthProbe(tracker, clock);
        this.writer = new RawTickWriter(flussWriter, tracker, config.rawTableName,
                config.appendTimeout,
                java.time.Duration.ofSeconds(30)); // drain deadline

        // Telemetry emitter — flushes every 10s to otel-collector:4318
        String otelHost = System.getenv().getOrDefault(
                "OTEL_COLLECTOR_HOST", "otel-collector:4318");
        this.metrics = new OtlpMetricsEmitter(otelHost, instanceId);
        metrics.setHealthCallback(health::setOtlpHealthy);
        metrics.start();
        // R-245: report the actual manifest version, not a hardcoded 1. All
        // instruments share the loaded manifest version (SCH-22).
        long manifestVersion = instruments.stream()
                .mapToLong(Instrument::manifestVersion)
                .max().orElse(0L);
        metrics.setManifestVersion(manifestVersion);

        // Quarantine + discontinuity writers (Phase 2b)
        this.quarantineWriter = new QuarantineWriter(config.flussBootstrap, instanceId);
        this.discontinuityWriter = new DiscontinuityWriter(
                config.flussBootstrap, instanceId, "arrow-bridge", connectionEpoch);

        // Safety writer (Phase 6A — slot-scoped safety propagation).
        this.manifestFingerprint = InstrumentManifestLoader.computeFingerprint(instruments);
        this.assignedTokenSetHash = com.trading.ingestion.safety.SafetyHaltWriter
                .computeAssignedTokenHash(instruments.stream()
                        .map(Instrument::instrumentToken).toList());
        String accountScope = System.getenv().getOrDefault("ACCOUNT_SCOPE_ID", "QP3796");
        this.safetyHaltWriter = new com.trading.ingestion.safety.SafetyHaltWriter(
                config.flussBootstrap, instanceId, manifestFingerprint, accountScope);
        String readinessPath = System.getenv("READINESS_FILE_PATH");
        this.readinessFile = readinessPath == null || readinessPath.isBlank()
                ? null : new ReadinessFile(java.nio.file.Paths.get(readinessPath));

        tracker.setListener((level, recs, byt, mr, mb, now) -> {
            if (level == AppendTracker.BackpressureListener.Level.WARNING) {
                LOG.warn("ingestion: backpressure warning (pending={} records, {} bytes)", recs, byt);
            } else {
                LOG.error("ingestion: backpressure critical — halting (pending={} records, {} bytes)", recs, byt);
            }
            // R-246: keep the readiness marker in sync during silent degradation
            // (backpressure/append failures) even when no bridge event arrives.
            updateReadinessFile();
        });
    }

    // ---- main entry point ----

    public static void main(String[] args) throws Exception {
        // 1. Validate ALL configuration keys (throws on failure)
        IngestionConfig config = IngestionConfig.validate();
        LOG.info("ingestion: config validated ({} keys, fluss={})",
                config.toMap().size(), config.flussBootstrap);

        String instanceId = "ingestion-" + System.getenv().getOrDefault("HOSTNAME", "local");

        LOG.info("ingestion: starting (instance={}, build={}, fluss={})",
                instanceId, VERSION, config.flussBootstrap);

        // 2. Check clock offset (C21, I9)
        NtpClockChecker clock = new NtpClockChecker(
                System.getenv().getOrDefault("NTP_SERVER",
                        "ntp.ubuntu.com,time.google.com,in.pool.ntp.org"),
                config.clockOffsetLimitMs,
                config.clockCheckRequired);
        try {
            long offset = clock.measureOffsetMs();
            LOG.info("ingestion: clock offset {}ms (limit {}ms, passed={})",
                    offset, config.clockOffsetLimitMs, clock.isWithinLimit());
            if (!clock.isWithinLimit() && config.clockCheckRequired) {
                LOG.error("ingestion: FATAL — clock offset outside limit and CLOCK_CHECK_REQUIRED=true");
                System.exit(1);
            }
        } catch (NtpClockChecker.NtpException e) {
            if (config.clockCheckRequired) {
                LOG.error("ingestion: FATAL — NTP clock check failed and CLOCK_CHECK_REQUIRED=true: {}",
                        e.getMessage());
                System.exit(1);
            }
            LOG.warn("ingestion: clock check failed: {} — falling back to wall-clock sanity", e.getMessage());
        }

        // 3. Schema verification — read-only by default; DDL mutation only when
        //    ALLOW_RUNTIME_DDL=true (local development only).
        boolean schemaOk = config.allowRuntimeDdl
                ? DdlBootstrap.ensureTables(config.flussBootstrap)
                : DdlBootstrap.verifyTables(config.flussBootstrap);
        if (!schemaOk) {
            LOG.error("ingestion: FATAL — Fluss schema verification failed (allowRuntimeDdl={}); "
                    + "run DDL first or set ALLOW_RUNTIME_DDL=true for local dev",
                    config.allowRuntimeDdl);
            System.exit(1);
        }

        // 4. Connect to Fluss and verify schema version (D3)
        FlussRowConverter converter = FlussClientAdapter.connect(
                config.flussBootstrap, config.rawTableName);
        LOG.info("ingestion: Fluss connected (bootstrap={}, table={})",
                config.flussBootstrap, config.rawTableName);

        // 4. Load instrument manifest (D4) — validate version + fingerprint (SCH-22)
        InstrumentManifestLoader.ManifestResult manifestResult =
                InstrumentManifestLoader.loadDefault();
        if (manifestResult.instruments().isEmpty()) {
            LOG.error("ingestion: FATAL — manifest load returned empty instrument set");
            System.exit(1);
        }
        LOG.info("ingestion: manifest loaded (instruments={}, approved={}, version={}, fingerprint={})",
                manifestResult.instrumentCount(), manifestResult.approved(),
                manifestResult.version(),
                manifestResult.fingerprint().substring(0, Math.min(12, manifestResult.fingerprint().length())));
        List<Instrument> instruments = manifestResult.instruments();

        // 5. Create the service
        IngestionService service = new IngestionService(
                instanceId, instruments, converter, config, clock);

        // 5a. Fluss schema verified + connected → mark Fluss readiness dimension.
        service.health().setFlussReady(true);

        // 5b. Uncertainty journal must be writable before we accept data.
        if (!service.journal.ensureWritable()) {
            LOG.error("ingestion: FATAL — uncertainty journal not writable; "
                    + "set UNCERTAINTY_JOURNAL_PATH to a writable path");
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("ingestion: shutdown requested");
            service.shutdown();
        }));

        // 6. Launch Go arrow-bridge and pipe NDJSON through Java (D7, I1, I6)
        String bridgeBin = System.getenv().getOrDefault("ARROW_BRIDGE_BIN", "/app/arrow-bridge");
        service.runWithBridge(bridgeBin);
    }

    // ---- bridge subprocess loop ----

    /**
     * Launch the Go arrow-bridge as a subprocess, read NDJSON from its stdout,
     * and pipe its stderr into SLF4J. This replaces the shell pipe
     * ({@code arrow-bridge | java}) with Java-managed lifecycle.
     *
     * <p>On bridge exit, records the exit code and (on non-zero) calls
     * {@link #shutdown()}.
     */
    public void runWithBridge(String bridgeBinary) {
        running = true;
        subscriptionPaused = false;

        LOG.info("ingestion: launching arrow-bridge (binary={}, instance={})",
                bridgeBinary, instanceId);

        // R-108: broker-staleness must be detected even while readLine() blocks
        // during a genuine feed outage — the ING-1 inline check only runs after
        // a new frame arrives. A watchdog thread evaluates staleness every 5s.
        stalenessWatchdog.scheduleAtFixedRate(() -> {
            if (!running) return;
            long nowNanos = System.nanoTime();
            long msSinceLastFrame = lastFrameNanos > 0
                    ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nowNanos - lastFrameNanos)
                    : 0;
            if (lastFrameNanos > 0 && msSinceLastFrame > FRAME_STALE_MS
                    && health.isBrokerConnected()) {
                LOG.warn("ingestion: broker data stale ({}ms since last frame) — marking disconnected",
                        msSinceLastFrame);
                health.setBrokerConnected(false);
                metrics.setBridgeConnected(false);
                updateReadinessFile();
            }
        }, 5, 5, java.util.concurrent.TimeUnit.SECONDS);

        // ---- Subscription completeness tracking (ING-2) ----
        java.util.Set<Long> seenTokens = java.util.concurrent.ConcurrentHashMap.newKeySet();
        long lastSubscriptionWarningNanos = System.nanoTime();

        // Start with broker not connected — set to true on first frame
        health.setBrokerConnected(false);
        health.setSubscriptionComplete(false);
        metrics.setBridgeConnected(false);

        int restartCount = 0;
        Process bridgeProcess = null;
        boolean bridgeLoopDone = false;

        while (running && restartCount <= MAX_BRIDGE_RESTARTS) {
            long bridgeStartNanos = System.nanoTime();
            lastSubscriptionWarningNanos = bridgeStartNanos;

            try {
                bridgeProcess = startBridge(bridgeBinary);
                final Process proc = bridgeProcess;

                Thread stderrThread = new Thread(() -> drainStderr(proc),
                        "bridge-stderr");
                stderrThread.setDaemon(true);
                stderrThread.start();

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(bridgeProcess.getInputStream(), StandardCharsets.UTF_8))) {

                    String line;
                    while (running && (line = reader.readLine()) != null) {
                        line = line.strip();
                        if (line.isEmpty()) continue;

                        long nowNanos = System.nanoTime();

                        // ---- ING-1: Broker disconnect detection via frame staleness ----
                        long msSinceLastFrame = (lastFrameNanos > 0)
                                ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nowNanos - lastFrameNanos)
                                : 0;
                        if (lastFrameNanos > 0 && msSinceLastFrame > FRAME_STALE_MS && health.isBrokerConnected()) {
                            LOG.warn("ingestion: broker data stale ({}ms since last frame) — marking disconnected",
                                    msSinceLastFrame);
                            health.setBrokerConnected(false);
                            metrics.setBridgeConnected(false);
                        }

                        // Record frame arrival
                        lastFrameNanos = nowNanos;
                        if (!health.isBrokerConnected()) {
                            String userInfo = config.arrowUserId.isBlank() ? "token" : config.arrowUserId;
                            LOG.info("ingestion: ✅ CONNECTED to Arrow Trade as user {} — broker data flowing", userInfo);
                            health.setBrokerConnected(true);
                            metrics.setBridgeConnected(true);
                        }
                        health.setLastFrameReceived(nowNanos);

                        // ---- ING-2: Subscription completeness check ----
                        if (!health.isSubscriptionComplete()) {
                            long msSinceStart = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                    nowNanos - bridgeStartNanos);
                            // Try to extract token from JSON (lightweight parse before full processLine)
                            try {
                                int idx = line.indexOf("\"token\"");
                                if (idx >= 0) {
                                    int colon = line.indexOf(':', idx);
                                    if (colon >= 0) {
                                        int start = colon + 1;
                                        while (start < line.length() && Character.isWhitespace(line.charAt(start))) start++;
                                        int end = start;
                                        while (end < line.length() && Character.isDigit(line.charAt(end))) end++;
                                        if (end > start) {
                                            long token = Long.parseLong(line.substring(start, end));
                                            seenTokens.add(token);
                                        }
                                    }
                                }
                            } catch (Exception ignore) { /* lightweight parse failure — skip */ }

                            if (msSinceStart > SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS) {
                                if (seenTokens.size() >= instrumentMap.size()) {
                                    LOG.info("ingestion: subscription complete ({} of {} tokens seen in {}ms)",
                                            seenTokens.size(), instrumentMap.size(), msSinceStart);
                                    health.setSubscriptionComplete(true);
                                } else {
                                    long msSinceLastWarning = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                            nowNanos - lastSubscriptionWarningNanos);
                                    if (msSinceLastWarning >= SUBSCRIPTION_COMPLETENESS_TIMEOUT_MS) {
                                        LOG.warn("ingestion: subscription incomplete after {}ms — {} of {} tokens seen",
                                                msSinceStart, seenTokens.size(), instrumentMap.size());
                                        lastSubscriptionWarningNanos = nowNanos;
                                    }
                                    health.setSubscriptionComplete(false);
                                }
                            }
                        }

                        // ---- ING-3: Slow-Fluss controlled pause ----
                        long pendingRecs = tracker.pendingRecords();
                        // R-109: the pause threshold must use the CONFIGURED max pending
                        // records (tracker.maxPendingRecords()), not the static 10,000 — a
                        // smaller configured limit would never reach the 90% pause.
                        long maxRecs = tracker.maxPendingRecords();
                        double pendingPct = (double) pendingRecs / maxRecs;

                        if (!subscriptionPaused && pendingPct >= SLOW_FLUSS_PAUSE_PERCENT) {
                            subscriptionPaused = true;
                            LOG.warn("ingestion: Slow-Fluss backpressure — pending at {}% ({} of {} records), "
                                    + "pausing subscription reads to protect memory",
                                    String.format("%.0f", pendingPct * 100), pendingRecs, maxRecs);
                            metrics.incrementAcknowledgedLoss();
                        }

                        if (subscriptionPaused) {
                            if (pendingPct <= SLOW_FLUSS_RESUME_PERCENT) {
                                subscriptionPaused = false;
                                LOG.info("ingestion: Slow-Fluss backpressure resolved — pending at {}% ({} records), "
                                        + "resuming subscription reads",
                                        String.format("%.0f", pendingPct * 100), pendingRecs);
                            } else {
                                // Skip processing this frame — queue is still draining
                                continue;
                            }
                        }

                        processLine(line);
                    }
                }

                int exitCode = bridgeProcess.waitFor();
                boolean requested = exitCode == 0 || !running;
                recordBridgeExit(exitCode, requested, restartCount);
                stderrThread.join(5_000);

                switch (bridgeRestartDecision(running, exitCode, restartCount)) {
                case RESTART:
                    restartCount++;
                    LOG.warn("ingestion: restarting bridge after unexpected exit (attempt {} of {})",
                            restartCount, MAX_BRIDGE_RESTARTS + 1);
                    // Wait 1 second before restart (plan).
                    try {
                        Thread.sleep(BRIDGE_RESTART_WAIT_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        bridgeLoopDone = true;
                        break;
                    }
                    // Reset slot states to AUTHENTICATING for the fresh process.
                    health.resetSlotsToAuthenticating();
                    // R-110: the fresh bridge process must re-establish
                    // subscription completeness from zero — tokens seen by the
                    // previous process must not count toward the new one.
                    seenTokens.clear();
                    health.setSubscriptionComplete(false);
                    metrics.setBridgeConnected(false);
                    break;
                case TERMINAL:
                    LOG.error("ingestion: bridge exited unexpectedly {} time(s) — terminal (exitCode={})",
                            restartCount + 1, exitCode);
                    bridgeLoopDone = true;
                    break;
                default: // NO_RESTART — normal shutdown
                    bridgeLoopDone = true;
                    break;
                }
                if (bridgeLoopDone) {
                    break;
                }

            } catch (Exception e) {
                LOG.error("ingestion: bridge process error", e);
                break;
            }
        }

        LOG.info("ingestion: bridge loop ended (ticks={}, errors={}, restarts={})",
                frameCount.get(), errorCount.get(), restartCount);
        shutdown();
    }

    /**
     * Start the Go arrow-bridge subprocess with the current environment.
     */
    private Process startBridge(String bridgeBinary) throws java.io.IOException {
        ProcessBuilder pb = new ProcessBuilder(bridgeBinary);
        pb.environment().putAll(System.getenv());
        pb.redirectErrorStream(false);
        Process process = pb.start();
        LOG.info("ingestion: arrow-bridge started (pid={})", process.pid());
        return process;
    }

    /**
     * Bridge restart policy (plan §IngestionService): an unexpected exit is
     * restarted once; a second unexpected exit in the same process is terminal;
     * a requested exit (code 0 or shutdown begun) is never restarted.
     */
    static BridgeRestartDecision bridgeRestartDecision(boolean running, int exitCode, int restartCount) {
        if (!running || exitCode == 0) return BridgeRestartDecision.NO_RESTART;
        return restartCount >= MAX_BRIDGE_RESTARTS
                ? BridgeRestartDecision.TERMINAL
                : BridgeRestartDecision.RESTART;
    }

    /** Outcome of the bridge restart policy. */
    enum BridgeRestartDecision { RESTART, TERMINAL, NO_RESTART }

    /**
     * Drain bridge stderr into SLF4J. Classifies each line per plan §log4j2:
     * lines containing `failed`, `error`, `ended`, `stalled`, `partial`, or
     * `rejected` log at WARN/ERROR; all other bridge diagnostics log at INFO.
     * Tick stdout is never logged.
     */
    private void drainStderr(Process process) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String safe = sanitizeLog(line);
                if (classifyBridgeLine(safe)) {
                    LOG.warn("arrow-bridge: {}", safe);
                } else {
                    LOG.info("arrow-bridge: {}", safe);
                }
            }
        } catch (Exception e) {
            LOG.warn("arrow-bridge: stderr drain error: {}", e.getMessage());
        }
    }

    /**
     * Plan §log4j2 classification: {@code true} → WARN/ERROR level, {@code false} → INFO.
     * Matches lines containing failed/error/ended/stalled/partial/rejected.
     */
    static boolean classifyBridgeLine(String line) {
        if (line == null || line.isBlank()) return false;
        String l = line.toLowerCase(java.util.Locale.ROOT);
        return l.contains("failed") || l.contains("error") || l.contains("ended")
                || l.contains("stalled") || l.contains("partial") || l.contains("rejected");
    }

    /**
     * Record a bridge process exit as discontinuity evidence (plan §IngestionService).
     *
     * @param exitCode      the process exit code
     * @param requested     true if the exit was requested (normal shutdown, code 0, or shutdown begun)
     * @param restartCount  how many restarts have already occurred
     */
    private void recordBridgeExit(int exitCode, boolean requested, int restartCount) {
        long newEpoch = connectionEpoch.incrementAndGet();
        if (requested) {
            LOG.info("ingestion: bridge exited normally (code={}, epoch={}, restarts={})",
                    exitCode, newEpoch, restartCount);
        } else {
            LOG.error("ingestion: BRIDGE_CRASH exitCode={} instance={} epoch={} restarts={}",
                    exitCode, instanceId, newEpoch, restartCount);
        }
        health.setBrokerConnected(false);
        metrics.setBridgeConnected(false);
        metrics.setChildProcessAlive(false);
        metrics.setActiveSockets(0);

        discontinuityWriter.write(
                DiscontinuityWriter.Reason.DROP,
                "arrow-bridge " + (requested ? "exited" : "crashed") + " with code " + exitCode,
                lastTickSnapshot,
                null // no after snapshot until reconnection
        );
    }

    private void processLine(String jsonLine) {
        frameCount.incrementAndGet();
        try {
            java.util.Optional<BridgeEvent> bridgeEvent = bridgeEventParser.parse(jsonLine);
            if (bridgeEvent.isPresent()) {
                handleBridgeEvent(bridgeEvent.get());
                return;
            }
            java.util.Optional<BrokerQuarantine> brokerQuarantine = bridgeEventParser.parseQuarantine(jsonLine);
            if (brokerQuarantine.isPresent()) {
                BrokerQuarantine record = brokerQuarantine.get();
                quarantineWriter.write(record.rawPayload(),
                        QuarantineWriter.Reason.valueOf(record.reason()),
                        "bridge broker quarantine",
                        record.token(), null, null);
                metrics.incrementDecodeError(record.reason());
                return;
            }
            // 1. Parse NDJSON → GoTick (same schema as Go bridge outputs)
            GoTick gt = MAPPER.readValue(jsonLine, GoTick.class);
            long receiveTsMs = gt.received_ts_ms > 0 ? gt.received_ts_ms : System.currentTimeMillis();
            byte[] rawBytes = jsonLine.getBytes(StandardCharsets.UTF_8);

            // 1a. Validate the original-bytes hash (plan §Tick / §Data Flow):
            //     raw_payload is the exact decompressed broker packet bytes
            //     (Base64); payload_hash is their SHA-256. Decoded JSON must not
            //     replace those bytes. A mismatch quarantines the record.
            byte[] packetBytes = decodeAndValidatePayload(gt, rawBytes);
            if (packetBytes == null) {
                return; // already quarantined
            }

            if (gt.ts_ms <= 0) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.INVALID_VALUES,
                        "missing or non-positive broker timestamp", gt.token, null, null);
                return;
            }
            // Freshness gate — stale or future broker timestamps are quarantined
            // BEFORE any trade classification, so stale data can never become a
            // trade decision. (Plan §Production hardening: 1376.)
            FreshnessDecision fd = classifyFreshness(gt.ts_ms, receiveTsMs,
                    config.arrowMaxFutureEventSkewMs, config.arrowMaxEventAgeMs);
            if (fd == FreshnessDecision.FUTURE) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP,
                        "event timestamp exceeds receive time", gt.token, null, null);
                return;
            }
            if (fd == FreshnessDecision.STALE) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP,
                        "event timestamp is older than configured age", gt.token, null, null);
                metrics.incrementDecodeError("STALE_BROKER_TIMESTAMP");
                return;
            }

            // 2. Resolve instrument
            Instrument instr = instrumentMap.get(gt.token);
            if (instr == null) {
                LOG.warn("ingestion: missing instrument token={}", gt.token);
                // E4, I3: Missing instrument → quarantine, don't silently skip
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.MISSING_INSTRUMENT,
                        "token=" + gt.token + " not found in daily manifest",
                        gt.token, null, null);
                metrics.incrementDecodeError("MISSING_INSTRUMENT");
                return;
            }

            // 3. Validate
            ValidityClassification validity;
            String validityReason = null;
            if (gt.ltp_paise <= 0 && (gt.mode.equals("ltp") || gt.mode.equals("ltpc"))) {
                validity = ValidityClassification.INVALID_VALUES;
                validityReason = "ltp_paise <= 0";
            } else if (gt.mode.equals("ltp") || gt.mode.equals("ltpc") || gt.mode.equals("full")) {
                validity = ValidityClassification.VALID_TRADE;
            } else {
                validity = ValidityClassification.VALID_NON_TRADE;
            }

            if (validity == ValidityClassification.INVALID_VALUES) {
                // E4: Invalid values → quarantine with instrument context
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.INVALID_VALUES,
                        validityReason,
                        instr.instrumentToken(), instr.exchange(), instr.tradingSymbol());
                metrics.incrementDecodeError("INVALID_VALUES");
                return;
            }

            if ("full".equals(gt.mode)
                    && gt.lower_limit_paise > 0 && gt.upper_limit_paise > 0
                    && (gt.ltp_paise < gt.lower_limit_paise || gt.ltp_paise > gt.upper_limit_paise)) {
                quarantineWriter.write(rawBytes, QuarantineWriter.Reason.BROKER_LIMIT_VIOLATION,
                        "ltp outside broker circuit limits",
                        instr.instrumentToken(), instr.exchange(), instr.tradingSymbol());
                return;
            }

            String tickType = (validity == ValidityClassification.VALID_TRADE) ? "TRADE" : "QUOTE";

            // 4. Fingerprint
            FingerprintBuilder.Result fp = FingerprintBuilder.build(
                    gt.connection_epoch,
                    gt.token,
                    gt.ts_ms,
                    tickType,
                    gt.ltp_paise,
                    gt.ltq,
                    gt.bid_px != null && gt.bid_px[0] != 0 ? gt.bid_px[0] : 0L,
                    gt.ask_px != null && gt.ask_px[0] != 0 ? gt.ask_px[0] : 0L
            );

            // 5. Build RawTick (raw_payload = exact decompressed broker packet
            //    bytes validated in step 1a, not the NDJSON line)
            RawTick raw = new RawTick.Builder()
                    .rawPayload(packetBytes)
                    .payloadHash(gt.payload_hash)
                    .hashAlgorithm(FINGERPRINT_ALGO)
                    .protocolVersion(gt.feed)
                    .decoderVersion("arrow-go-sdk")
                    .receiveTime(Instant.now())
                    .receiveTimeNanos(System.nanoTime())
                    .build();

            // 6. Build TickPacket
            TickPacket packet = new TickPacket.Builder()
                    .raw(raw)
                    .validity(validity)
                    .validityReason(validityReason)
                    .instrumentToken(gt.token)
                    .tradingSymbol(instr.tradingSymbol())
                    .exchange(instr.exchange())
                    .eventTime(Instant.ofEpochMilli(gt.ts_ms))
                    .ingestTs(Instant.now())
                    .lastPricePaise(gt.ltp_paise)
                    .volume(gt.volume)
                    .ohlcOpenPaise(gt.open_paise)
                    .ohlcHighPaise(gt.high_paise)
                    .ohlcLowPaise(gt.low_paise)
                    .ohlcClosePaise(gt.close_paise)
                    .eventFingerprint(fp.hash())
                    .fingerprintVersion(fp.version())
                    .connectionId(gt.connection_id == null || gt.connection_id.isBlank() ? "arrow-bridge" : gt.connection_id)
                    .connectionEpoch(gt.connection_epoch)
                    .instanceId(instanceId)
                    .schemaVersion(1)
                    .build();

            // 7. Submit to bounded writer (individual append, no batching)
            RawTickWriter.AppendOutcome outcome = writer.write(packet);

            // Record metrics
            metrics.recordTick(packetBytes.length);
            long latencyMs = outcome.ackTime() != null
                    ? java.time.Duration.between(outcome.acceptTime(), outcome.ackTime()).toMillis()
                    : -1;
            if (latencyMs >= 0) metrics.recordAppendLatencyMs(latencyMs);
            metrics.incrementFingerprint();

            if (outcome.status() == RawTickWriter.Status.REJECTED) {
                LOG.warn("ingestion: tick rejected (reason={})", outcome.detail());
                metrics.incrementAcknowledgedLoss();
            } else if (outcome.status() == RawTickWriter.Status.TIMEOUT) {
                LOG.warn("ingestion: append timeout (rowBytes={})", outcome.rowBytes());
            } else if (outcome.status() == RawTickWriter.Status.UNCERTAIN) {
                LOG.warn("ingestion: append UNCERTAIN — Fluss may have persisted (rowBytes={}, detail={})",
                        outcome.rowBytes(), outcome.detail());
                errorCount.incrementAndGet();
            } else if (outcome.status() == RawTickWriter.Status.FAILED
                    || outcome.status() == RawTickWriter.Status.FATAL) {
                errorCount.incrementAndGet();
                metrics.incrementDecodeError("append_" + outcome.status().name().toLowerCase());
            }

            // 8. Update health probe + gauge metrics + lastTickSnapshot
            health.setLastFrameReceived(System.nanoTime());
            metrics.setPendingRecords(tracker.pendingRecords());
            metrics.setPendingBytes(tracker.pendingBytes());
            metrics.setIngestionReady(health.isReady());
            metrics.setBridgeConnected(true);
            refreshResourceMetrics();
            // R-246: the readiness marker must track the tick-processing path
            // too, not just bridge lifecycle events.
            updateReadinessFile();

            // R-111: lastTickSnapshot is discontinuity evidence — it must only
            // reflect a tick that was actually persisted. REJECTED/TIMEOUT/
            // UNCERTAIN/FAILED/FATAL ticks were not (or may not have been)
            // appended; recording them would fabricate false "last accepted
            // tick" evidence after a later bridge crash.
            if (outcome.status() == RawTickWriter.Status.SUCCESS) {
                lastTickSnapshot = new DiscontinuityWriter.LastTickSnapshot(
                        gt.ts_ms,
                        fp.hash(),
                        gt.token,
                        instr.exchange(),
                        instr.tradingSymbol()
                );
            }

        } catch (JsonProcessingException e) {
            // Non-JSON line (e.g. Go bridge startup output). Silently skip —
            // bridge stderr is already drained to SLF4J at DEBUG level.
        } catch (Exception e) {
            errorCount.incrementAndGet();
            metrics.incrementDecodeError(e.getClass().getSimpleName());
            // Write quarantine record for any unhandled processing failure
            try {
                byte[] rawBytes = jsonLine != null ? jsonLine.getBytes(StandardCharsets.UTF_8) : null;
                quarantineWriter.write(rawBytes,
                        QuarantineWriter.Reason.INTERNAL_ERROR,
                        e.getClass().getSimpleName() + ": " + e.getMessage());
            } catch (Exception nested) {
                LOG.error("ingestion: quarantine writer failed: {}", nested.getMessage());
            }
            LOG.warn("ingestion: line processing error: {}", e.getMessage());
        }
    }

    /**
     * Route a bridge lifecycle event: update health/metrics and persist
     * discontinuity evidence per the plan's mapping.
     */
    private void processBridgeEvent(BridgeEvent event) {
        boolean active = "ACTIVE".equals(event.state())
                && event.assignedTokens() == event.acknowledgedTokens()
                && event.rejectedTokens() == 0;
        health.setBrokerConnected(active);
        health.setSubscriptionComplete(active);
        if (active) health.setLastFrameReceived(System.nanoTime());
        health.updateSlot(event.slotId(), event.state(), event.connectionEpoch(),
                event.assignedTokens(), event.acknowledgedTokens(), event.rejectedTokens(),
                active ? System.nanoTime() : 0L);
        metrics.setConnectionEpoch(event.connectionEpoch());
        // Slot gauges (plan §Monitoring — slot label only, no token/symbol).
        metrics.setSlotState(event.slotId(), active, event.assignedTokens(),
                event.acknowledgedTokens(), event.rejectedTokens(),
                active ? System.nanoTime() : 0L);
        metrics.setSlotCapacityUsedPercent(event.slotId(),
                event.assignedTokens() > 0
                        ? (100.0 * event.acknowledgedTokens()) / event.assignedTokens()
                        : 0.0);
        metrics.setActiveSockets(active ? 1 : 0);
        metrics.setChildProcessAlive(true);

        // ---- Slot-scoped safety propagation (plan Amendment §Slot-scoped safety) ----
        emitSafetyTransition(event, active);

        // ---- Discontinuity evidence (plan §DiscontinuityWriter) ----
        // Reconnect evidence is written only when the slot reaches ACTIVE again.
        if ("reconnect".equals(event.event()) && active) {
            discontinuityWriter.writeBridgeEvent(event, lastTickSnapshot);
        } else if (isEvidenceEvent(event.event())) {
            discontinuityWriter.writeBridgeEvent(event, lastTickSnapshot);
        }

        if ("reconnect".equals(event.event())) {
            metrics.incrementSubscriptionRetry();
            if (event.reason() != null && event.reason().contains("authentication_refreshed")) {
                metrics.incrementAuthRefresh();
            }
        }
        if ("heartbeat_failed".equals(event.event())) metrics.incrementHeartbeatFailure();
        if ("feed_stalled".equals(event.event())) metrics.incrementFeedStall();
        if ("auth_failure".equals(event.event())) metrics.incrementAuthFailure();
        if ("subscription_ack".equals(event.event()) && event.rejectedTokens() > 0) {
            metrics.incrementPartialSubscription();
            // Partial acknowledgement is feed-health evidence.
            discontinuityWriter.writeBridgeEvent(event, lastTickSnapshot);
        }
        if ("bridge_shutdown".equals(event.event())) {
            LOG.info("ingestion: bridge requested shutdown (slot={}, epoch={})", event.slotId(), event.connectionEpoch());
            running = false;
        } else if ("auth_failure".equals(event.event())) {
            LOG.error("ingestion: bridge authentication failed (slot={}, epoch={}, reason={})",
                    event.slotId(), event.connectionEpoch(), sanitizeLog(event.reason()));
        } else if ("feed_stalled".equals(event.event()) || "heartbeat_failed".equals(event.event())) {
            health.setBrokerConnected(false);
            metrics.setBridgeConnected(false);
        }
        updateReadinessFile();
        LOG.info("bridge lifecycle event={} slot={} state={} epoch={} assigned={} acknowledged={} rejected={} reason={}",
                event.event(), event.slotId(), event.state(), event.connectionEpoch(),
                event.assignedTokens(), event.acknowledgedTokens(), event.rejectedTokens(),
                sanitizeLog(event.reason()));
    }

    /** Whether a lifecycle event should be recorded as discontinuity evidence. */
    private static boolean isEvidenceEvent(String eventName) {
        return switch (eventName) {
            case "disconnect", "bridge_exit", "auth_failure",
                 "heartbeat_failed", "feed_stalled" -> true;
            default -> false;
        };
    }

    /**
     * Emit slot-scoped safety requests (plan Amendment §Slot-scoped safety
     * propagation). One UNSAFE row exactly once per transition; a RECOVERED
     * row only when the slot returns to ACTIVE with full acknowledgement. Rows
     * are deduped by the computed halt_request_id (same tuple never re-emitted).
     */
    private void emitSafetyTransition(BridgeEvent event, boolean active) {
        if (safetyHaltWriter == null) return;
        String slotId = event.slotId();
        long epoch = event.connectionEpoch();

        com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode unsafe = null;
        switch (event.event()) {
            case "feed_stalled" ->
                unsafe = "decode_error_burst".equals(event.reason())
                        ? com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.DECODE_ERROR_BURST
                        : com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.FEED_STALLED;
            case "heartbeat_failed" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.HEARTBEAT_FAILED;
            case "disconnect" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.READ_FAILURE;
            case "auth_failure" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.AUTH_FAILURE;
            case "bridge_shutdown", "bridge_exit" ->
                unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.BRIDGE_EXIT;
            case "subscription_ack" -> {
                if (event.rejectedTokens() > 0 && !active) {
                    unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.SUBSCRIPTION_PARTIAL;
                } else if ("TERMINAL".equals(event.state())
                        && "subscription_response_timeout".equals(event.reason())) {
                    unsafe = com.trading.ingestion.safety.SafetyHaltWriter.ReasonCode.SUBSCRIPTION_TIMEOUT;
                }
            }
            default -> { /* slot_state / reconnect handled below */ }
        }

        if (unsafe != null) {
            String id = safetyHaltWriter.write(slotId, epoch,
                    com.trading.ingestion.safety.SafetyHaltWriter.SafetyState.UNSAFE,
                    unsafe, assignedTokenSetHash, event.event(), System.currentTimeMillis());
            if (safetyEmitted.add("UNSAFE|" + id)) {
                LOG.warn("safety: slot {} UNSAFE (reason={}, epoch={}, halt={})",
                        slotId, unsafe, epoch, id.substring(0, Math.min(8, id.length())));
            }
        } else if (active && "subscription_ack".equals(event.event())) {
            // Full acknowledgement → RECOVERED (same or greater epoch, frame present).
            String id = safetyHaltWriter.write(slotId, epoch,
                    com.trading.ingestion.safety.SafetyHaltWriter.SafetyState.RECOVERED,
                    null, assignedTokenSetHash, event.event(), System.currentTimeMillis());
            if (safetyEmitted.add("RECOVERED|" + id)) {
                LOG.info("safety: slot {} RECOVERED (epoch={}, halt={})",
                        slotId, epoch, id.substring(0, Math.min(8, id.length())));
            }
        }
    }

    private void handleBridgeEvent(BridgeEvent event) {
        processBridgeEvent(event);
    }

    private static final java.util.regex.Pattern SECRET_LOG_PATTERN = java.util.regex.Pattern.compile(
            "(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\\s,}]+)");
    private static final java.util.regex.Pattern BEARER_LOG_PATTERN = java.util.regex.Pattern.compile(
            "(?i)\\bBearer[=:\\s]+[^\\s,}]+");

    private static String sanitizeLog(String value) {
        if (value == null) return "";
        // Two-pass: Bearer tokens first (space-separated), then name=value pairs.
        String sanitized = BEARER_LOG_PATTERN.matcher(value).replaceAll("Bearer=[REDACTED]");
        sanitized = SECRET_LOG_PATTERN.matcher(sanitized).replaceAll("$1=[REDACTED]");
        return sanitized.length() > 512 ? sanitized.substring(0, 512) : sanitized;
    }

    private void updateReadinessFile() {
        if (readinessFile == null) return;
        try {
            readinessFile.setReady(health.isReady());
        } catch (java.io.IOException e) {
            LOG.warn("ingestion: readiness marker update failed: {}", sanitizeLog(e.getMessage()));
        }
    }

    // ---- shutdown ----

    private void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            LOG.debug("ingestion: duplicate shutdown ignored");
            return;
        }
        running = false;
        // Stop the staleness watchdog (R-108).
        stalenessWatchdog.shutdownNow();
        if (readinessFile != null) {
            try { readinessFile.clear(); }
            catch (java.io.IOException e) { LOG.warn("ingestion: readiness marker clear failed: {}", sanitizeLog(e.getMessage())); }
        }
        health.markNotAlive();

        // J3, I10: Persist uncertainty counters before drain
        journal.write(new UncertaintyJournal.Entry(
                instanceId,
                Instant.now(),
                tracker.totalAccepted(),
                tracker.totalAppended(),
                tracker.totalFailed(),
                tracker.totalRejected(),
                tracker.totalBytesAccepted(),
                "shutdown"
        ));

        // Close metrics emitter (triggers final flush)
        metrics.close();

        // Close quarantine + discontinuity writers
        if (quarantineWriter != null) quarantineWriter.close();
        if (discontinuityWriter != null) discontinuityWriter.close();
        if (safetyHaltWriter != null) safetyHaltWriter.close();

        // J2: Drain pending writes with deadline
        if (writer != null) writer.close();
        LOG.info("ingestion: drained (totalTicks={}, errors={})",
                frameCount.get(), errorCount.get());
    }

    // ---- health accessor ----

    public HealthProbe health() { return health; }
    public AppendTracker tracker() { return tracker; }

    /** Freshness classification for a broker timestamp vs. receive time. */
    enum FreshnessDecision { FRESH, STALE, FUTURE }

    /**
     * Pure freshness gate: a tick whose broker timestamp is older than
     * maxEventAgeMs (STALE) or ahead of receive time by more than
     * maxFutureSkewMs (FUTURE) is quarantined before any trade classification,
     * so stale data can never reach a trade decision. (Plan §Production
     * hardening 1376.)
     */
    static FreshnessDecision classifyFreshness(long tsMs, long receiveTsMs,
            long maxFutureSkewMs, long maxEventAgeMs) {
        if (receiveTsMs - tsMs > maxEventAgeMs) return FreshnessDecision.STALE;
        if (tsMs - receiveTsMs > maxFutureSkewMs) return FreshnessDecision.FUTURE;
        return FreshnessDecision.FRESH;
    }

    // ---- GoTick JSON model (matches arrow-bridge output) ----

    /**
     * Mirrors the NDJSON schema emitted by the Go arrow-bridge.
     * All prices in paise, all timestamps in epoch ms.
     */
    @SuppressWarnings("unused")
    private static final class GoTick {
        @com.fasterxml.jackson.annotation.JsonProperty("record_type")
        public String record_type;
        @com.fasterxml.jackson.annotation.JsonProperty("connection_id")
        public String connection_id;
        @com.fasterxml.jackson.annotation.JsonProperty("connection_epoch")
        public long connection_epoch;
        @com.fasterxml.jackson.annotation.JsonProperty("slot_id")
        public String slot_id;
        @com.fasterxml.jackson.annotation.JsonProperty("received_ts_ms")
        public long received_ts_ms;
        @com.fasterxml.jackson.annotation.JsonProperty("feed_sequence_local")
        public long feed_sequence_local;
        @com.fasterxml.jackson.annotation.JsonProperty("raw_payload")
        public String raw_payload;
        @com.fasterxml.jackson.annotation.JsonProperty("payload_hash")
        public String payload_hash;
        public String feed;
        public String mode;
        public long token;
        @com.fasterxml.jackson.annotation.JsonProperty("ltp_paise")
        public long ltp_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("close_paise")
        public long close_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("open_paise")
        public long open_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("high_paise")
        public long high_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("low_paise")
        public long low_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("vwap_paise")
        public long vwap_paise;
        public long ltq;
        public long volume;
        @com.fasterxml.jackson.annotation.JsonProperty("total_buy_qty")
        public long total_buy_qty;
        @com.fasterxml.jackson.annotation.JsonProperty("total_sell_qty")
        public long total_sell_qty;
        public long atv;
        public long btv;
        @com.fasterxml.jackson.annotation.JsonProperty("open_interest")
        public long open_interest;
        @com.fasterxml.jackson.annotation.JsonProperty("ts_ms")
        public long ts_ms;
        @com.fasterxml.jackson.annotation.JsonProperty("bid_px")
        public long[] bid_px;
        @com.fasterxml.jackson.annotation.JsonProperty("ask_px")
        public long[] ask_px;
        @com.fasterxml.jackson.annotation.JsonProperty("bid_qty")
        public long[] bid_qty;
        @com.fasterxml.jackson.annotation.JsonProperty("ask_qty")
        public long[] ask_qty;
        @com.fasterxml.jackson.annotation.JsonProperty("change_flag")
        public int change_flag;
        @com.fasterxml.jackson.annotation.JsonProperty("avg_price_paise")
        public long avg_price_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("lower_limit_paise")
        public long lower_limit_paise;
        @com.fasterxml.jackson.annotation.JsonProperty("upper_limit_paise")
        public long upper_limit_paise;
    }

    // ---- helpers ----

    /**
     * Refresh resource gauges (plan Amendment §Resource): FDs, RSS, JVM threads.
     * Go goroutines are reported by the bridge; child-process-alive is set by
     * lifecycle events. Bounded cost — called once per processed frame.
     */
    private void refreshResourceMetrics() {
        // R-140: /proc + ThreadMXBean reads are expensive at HFT tick rates —
        // throttle to at most one refresh every 5 seconds. The gauges are
        // slow-moving resources; a 5s staleness is irrelevant.
        long nowNanos = System.nanoTime();
        if (lastResourceRefreshNanos != 0
                && nowNanos - lastResourceRefreshNanos < 5_000_000_000L) {
            return;
        }
        lastResourceRefreshNanos = nowNanos;

        long open = countOpenFds();
        long limit = readFdLimit();
        metrics.setProcessOpenFds(open);
        metrics.setProcessFdLimit(limit);
        metrics.setProcessFdUsagePercent(limit > 0 ? 100.0 * open / limit : 0.0);
        metrics.setProcessRssBytes(readRssBytes());
        try {
            metrics.setJvmThreadsLive(
                    java.lang.management.ManagementFactory.getThreadMXBean().getThreadCount());
        } catch (Exception ignore) { /* skip */ }
    }

    /** Count open FDs via /proc/self/fd (Linux); -1 on non-Linux or failure. */
    private static long countOpenFds() {
        try (var stream = java.nio.file.Files.list(java.nio.file.Paths.get("/proc/self/fd"))) {
            return stream.count();
        } catch (Exception ignore) { /* non-Linux or unreadable */ }
        return -1L;
    }

    /**
     * Read the process's per-process RLIMIT_NOFILE soft limit from
     * {@code /proc/self/limits} (R-192) — NOT the system-wide
     * {@code /proc/sys/fs/file-max}, which made fd_usage_percent look near 0%
     * even at the real cap. Returns -1 on non-Linux or failure.
     */
    private static long readFdLimit() {
        try {
            java.nio.file.Path p = java.nio.file.Paths.get("/proc/self/limits");
            if (!java.nio.file.Files.isReadable(p)) return -1L;
            for (String line : java.nio.file.Files.readAllLines(p)) {
                if (line.startsWith("Max open files")) {
                    // Format: "Max open files  1024  1048576  files"
                    //            [0]    [1]   [2]   [3]     [4]
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 4 && !"unlimited".equals(parts[3])) {
                        return Long.parseLong(parts[3]); // soft limit
                    }
                }
            }
        } catch (Exception ignore) { /* non-Linux or unreadable */ }
        return -1L;
    }

    /** Read VmRSS from /proc/self/status (Linux); 0 on non-Linux or failure. */
    private static long readRssBytes() {
        try {
            java.nio.file.Path status = java.nio.file.Paths.get("/proc/self/status");
            if (!java.nio.file.Files.isReadable(status)) return 0L;
            for (String line : java.nio.file.Files.readAllLines(status)) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2) {
                        return Long.parseLong(parts[1]) * 1024L; // kB → bytes
                    }
                }
            }
        } catch (Exception ignore) { /* non-Linux or unreadable */ }
        return 0L;
    }

    /**
     * Decode the Base64 {@code raw_payload} from a tick and verify its SHA-256
     * digest equals the bridge-provided {@code payload_hash}.
     *
     * @return the exact decompressed broker packet bytes, or {@code null} if the
     *         record must be quarantined (bad Base64, empty payload, or hash
     *         mismatch). Quarantining is performed here; the caller returns.
     */
    private byte[] decodeAndValidatePayload(GoTick gt, byte[] rawLine) {
        PayloadHashValidator.Result[] result = new PayloadHashValidator.Result[1];
        byte[] packet = PayloadHashValidator.validate(gt.raw_payload, gt.payload_hash, result);
        if (packet != null) {
            return packet;
        }
        String detail = switch (result[0]) {
            case MALFORMED_PAYLOAD -> "raw_payload missing, not valid Base64, or empty";
            case MALFORMED_HASH -> "payload_hash missing or not a SHA-256 hex digest";
            case HASH_MISMATCH -> "payload hash mismatch: broker packet bytes do not match payload_hash";
            case VALID -> "unreachable";
        };
        quarantineWriter.write(rawLine, QuarantineWriter.Reason.HASH_MISMATCH,
                detail, gt.token, null, null);
        metrics.incrementDecodeError("HASH_MISMATCH");
        return null;
    }
}
