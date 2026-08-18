package com.trading.execution.gateway;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/** Fluss raw-client implementation of {@link IntentDedupStore}. */
public final class FlussIntentDedupStore implements IntentDedupStore {
    private static final String TABLE = "Execution_Intent_Processed";
    private final Connection connection;
    private final Table table;
    private final Duration timeout;

    public static FlussIntentDedupStore open(GatewayConfig config) {
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", config.flussBootstrap());
            Connection connection = ConnectionFactory.createConnection(c);
            Table table = connection.getTable(TablePath.of(config.flussDatabase(), TABLE));
            return new FlussIntentDedupStore(connection, table, config.requestTimeout());
        } catch (Exception e) {
            throw new IllegalStateException("cannot open Execution_Intent_Processed", e);
        }
    }
    FlussIntentDedupStore(Connection connection, Table table, Duration timeout) {
        this.connection = connection; this.table = table; this.timeout = timeout;
    }

    @Override public Map<String, String> hydrate() throws Exception {
        Map<String, String> out = new HashMap<>();
        var info = table.getTableInfo();
        for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(new TableBucket(info.getTableId(), bucket));
                 CloseableIterator<InternalRow> it = scanner.pollBatch(timeout)) {
                while (it.hasNext()) {
                    InternalRow r = it.next();
                    out.put(r.getString(0).toString(),
                            r.getString(1).toString());
                }
            }
        }
        return out;
    }

    @Override public void record(String instructionId, String requestHash, Long logOffset) throws Exception {
        GenericRow row = GenericRow.of(BinaryString.fromString(instructionId),
                BinaryString.fromString(requestHash), System.currentTimeMillis(),
                logOffset == null ? null : logOffset, BinaryString.fromString("1"));
        UpsertWriter writer = table.newUpsert().createWriter();
        try { writer.upsert(row).get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        finally { writer.flush(); }
    }

    @Override public void close() throws Exception { table.close(); connection.close(); }
}
