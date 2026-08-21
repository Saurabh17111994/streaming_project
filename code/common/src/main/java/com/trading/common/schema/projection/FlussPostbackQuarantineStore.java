package com.trading.common.schema.projection;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

/**
 * Fluss-backed immutable {@link PostbackQuarantineStore} for Postback_Quarantine (T1, closes in
 * WP-4 step 3). Mirrors {@link FlussProjectionLedgerStore}: keeps the in-memory view for
 * {@link #all()} but persists each append to the LOG table via {@link AppendWriter}. Row layout
 * follows 16_postback_quarantine.sql (13 cols). Live durability is proven by the env-gated
 * {@code FlussPostbackQuarantineStoreIntegrationTest} (log-scan read-back).
 */
public final class FlussPostbackQuarantineStore implements PostbackQuarantineStore, AutoCloseable {
    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
    private final InMemoryPostbackQuarantineStore delegate = new InMemoryPostbackQuarantineStore();

    private FlussPostbackQuarantineStore(Connection connection, Table table, long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
    }

    public static FlussPostbackQuarantineStore open(String bootstrap, String database,
                                                    String tableName, Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Table table = connection.getTable(TablePath.of(database, tableName));
            return new FlussPostbackQuarantineStore(connection, table, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override public void close() throws Exception { connection.close(); }

    @Override public void append(QuarantinedPostback row) throws Exception {
        delegate.append(row);
        Object[] v = new Object[13];
        v[0] = BinaryString.fromString(row.quarantineId());
        v[1] = BinaryString.fromString(row.postbackEventId());
        v[2] = BinaryString.fromString(row.reason().name());
        v[3] = row.originalPayload() == null ? new byte[0] : row.originalPayload();
        v[4] = BinaryString.fromString(row.payloadHash());
        v[5] = BinaryString.fromString(row.brokerOrderId());
        v[6] = BinaryString.fromString(row.instructionId());
        v[7] = BinaryString.fromString(row.correlationAttempt());
        v[8] = BinaryString.fromString(row.disposition());
        v[9] = BinaryString.fromString(row.dispositionReason());
        v[10] = row.quarantinedTs();
        v[11] = row.dispositionTs() == null ? null : row.dispositionTs();
        v[12] = BinaryString.fromString(row.schemaVersion());
        AppendWriter writer = table.newAppend().createWriter();
        try {
            writer.append(GenericRow.of(v)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }

    @Override public List<QuarantinedPostback> all() { return delegate.all(); }
}
