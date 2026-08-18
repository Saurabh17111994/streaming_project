package com.trading.execution.gateway;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

/** Explicit serializers for normalized execution images and independent writes. */
public final class FlussProjectionWriter implements ProjectionWriter {
    private final Connection connection;
    private final GatewayConfig config;
    private final Duration timeout;
    private final Map<String, Table> tables = new HashMap<>();

    public static FlussProjectionWriter open(GatewayConfig config) {
        try {
            Configuration c = new Configuration(); c.setString("bootstrap.servers", config.flussBootstrap());
            return new FlussProjectionWriter(ConnectionFactory.createConnection(c), config,
                    config.requestTimeout());
        } catch (Exception e) { throw new IllegalStateException("cannot open projection writer", e); }
    }
    FlussProjectionWriter(Connection connection, GatewayConfig config, Duration timeout) {
        this.connection = connection; this.config = config; this.timeout = timeout;
    }

    @Override public void writeAudit(NormalizedExecutionEvent e) throws Exception {
        if (e.audit() != null) append("Execution_Audit", auditRow(e));
    }
    @Override public void writeLifecycle(NormalizedExecutionEvent e) throws Exception {
        if (e.fill() != null) append("Fills", fillRow(e));
        if (e.lifecycle() != null) upsert("Order_Lifecycle", lifecycleRow(e));
        if (e.correlation() != null) upsert("Order_Correlation", correlationRow(e));
    }
    @Override public void writePosition(NormalizedExecutionEvent e) throws Exception {
        if (e.position() != null) upsert("Positions", positionRow(e));
    }

    private void append(String name, GenericRow row) throws Exception {
        AppendWriter writer = table(name).newAppend().createWriter();
        try { writer.append(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        finally { writer.flush(); }
    }
    private void upsert(String name, GenericRow row) throws Exception {
        UpsertWriter writer = table(name).newUpsert().createWriter();
        try { writer.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        finally { writer.flush(); }
    }

    private GenericRow auditRow(NormalizedExecutionEvent e) {
        var a = e.audit();
        return GenericRow.of(bs(a.auditEventId()), bs(e.eventType()), bs(nullable(e.correlation(), true)),
                bs(nullableAttempt(e)), bs(e.executionPartitionId()), bs(e.accountScopeId()), e.gateEpoch(),
                bs(e.actorId()), bs(a.evidenceHash()), bs(a.evidenceSummary()), e.eventTs(), bs("2"));
    }
    private GenericRow fillRow(NormalizedExecutionEvent e) {
        var f = e.fill();
        return GenericRow.of(bs(e.postbackEventId()), bs(f.postbackFingerprint()), bs(f.fingerprintVersion()),
                bs(e.accountScopeId()), bs(f.brokerOrderId()), bs(f.instructionId()), bs(f.executionAttemptId()),
                bs(f.tradeContextId()), bs(f.orderStatus()), f.cumulativeQty(), f.pendingQty(), f.fillQty(),
                f.fillPricePaise(), bs(f.fillId()), f.brokerEventTime(), f.receiveTime(), f.ingestTs(),
                f.originalPayload() == null ? new byte[0] : f.originalPayload(), bs(f.payloadHash()),
                bs(f.correlationState()), bs(f.correlationReason()), bs(f.decoderVersion()), bs("2"));
    }
    private GenericRow lifecycleRow(NormalizedExecutionEvent e) {
        var l = e.lifecycle();
        return GenericRow.of(bs(e.accountScopeId()), bs(l.brokerOrderId()), bs(l.instructionId()),
                bs(l.executionAttemptId()), bs(l.tradeContextId()), bs(l.normalizedState()), l.cumulativeQty(),
                l.pendingQty(), l.averageFillPricePaise(), bs(e.postbackEventId()), l.sourceVersion(),
                l.sourceEventTime(), l.lastReceiveTime(), bs(l.correlationState()), bs("2"));
    }
    private GenericRow positionRow(NormalizedExecutionEvent e) {
        var p = e.position();
        return GenericRow.of(bs(p.positionId()), bs(p.tradeContextId()), bs(e.accountScopeId()), p.instrumentToken(),
                bs(p.exchange()), bs(p.symbol()), bs(p.side()), bs(p.state()), p.openQuantity(), p.closedQuantity(),
                p.averageEntryPaise(), p.averageExitPaise(), bs(e.postbackEventId()), p.sourceVersion(),
                p.createdTs(), p.lastUpdateTs(), bs("2"));
    }
    private GenericRow correlationRow(NormalizedExecutionEvent e) {
        var c = e.correlation();
        return GenericRow.of(bs(c.instructionId()), bs(c.executionAttemptId()), bs(e.accountScopeId()),
                bs(c.clientOrderRef()), bs(c.brokerOrderId()), bs(c.tradeContextId()), bs(c.positionId()),
                bs(c.verificationState()), bs(c.verificationEvidence()), c.correlatedTs(), bs("2"));
    }
    private static String nullable(NormalizedExecutionEvent.Correlation c, boolean instruction) {
        return c == null ? null : c.instructionId();
    }
    private static String nullableAttempt(NormalizedExecutionEvent e) {
        return e.correlation() == null ? null : e.correlation().executionAttemptId();
    }
    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }
    private Table table(String name) { return tables.computeIfAbsent(name,
            n -> connection.getTable(TablePath.of(config.flussDatabase(), n))); }
    @Override public void close() throws Exception { for (Table t : tables.values()) t.close(); connection.close(); }
}
