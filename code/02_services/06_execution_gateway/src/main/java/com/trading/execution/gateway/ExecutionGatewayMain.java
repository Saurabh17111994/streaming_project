package com.trading.execution.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Process entry point. Startup is intentionally HALTED until Fluss and protocol are proven ready. */
public final class ExecutionGatewayMain {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGatewayMain.class);
    private ExecutionGatewayMain() {}

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromEnvironment();
        GatewayReadiness readiness = new GatewayReadiness();
        try (FlussControlStateStore controls = FlussControlStateStore.open(config);
                FlussProjectionWriter projections = FlussProjectionWriter.open(config);
                FlussProjectionLedgerStore ledger = FlussProjectionLedgerStore.open(config)) {
            ProjectionApplier applier = new ProjectionApplier(projections, ledger);
            NautilusIntentClient outbound = new NautilusIntentClient(config, controls, readiness);
            IntentReader reader = IntentReader.open(config, outbound,
                    reason -> { readiness.fail(reason); LOG.error("execution intent halted: {}", reason); });
            reader.subscribeFromBeginning();
            readiness.fluss(true, "Fluss tables opened");
            readiness.protocol(true, "private protocol configured");
            readiness.durableWrites(true, "projection ledger opened");
            ObjectMapper mapper = new ObjectMapper();
            try (GatewayHttpServer server = new GatewayHttpServer(config, readiness,
                    payload -> {
                        try { applier.apply(mapper.treeToValue(payload, NormalizedExecutionEvent.class)); }
                        catch (Exception e) { readiness.durableWrites(false, e.getMessage()); throw new IllegalStateException(e); }
                    })) {
                LOG.warn("execution-gateway started; execution readiness still depends on Execution_Gate=ENABLED");
                Thread readerThread = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try { reader.poll(config.pollTimeout()); }
                        catch (RuntimeException e) { readiness.fail(e.getMessage()); LOG.error("intent reader stopped", e); break; }
                    }
                }, "execution-intent-reader");
                readerThread.setDaemon(true);
                readerThread.start();
                server.start();
                Thread.currentThread().join();
            } finally { reader.close(); }
        }
    }
}
