package com.trading.execution.gateway;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded, replayable single-writer reader for the Execution_Intent LOG. */
public final class IntentReader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IntentReader.class);
    private final Connection connection;
    private final Table table;
    private final LogScanner scanner;
    private final GatewayConfig config;
    private final IntentSink forwarder;
    private final Consumer<String> violationHandler;
    private final IntentDedupStore dedupStore;
    private final DurableIntentDispatcher dispatcher;
    private final Map<Integer, Long> lastOffsets = new HashMap<>();

    public static IntentReader open(GatewayConfig config, IntentSink forwarder,
            Consumer<String> violationHandler) {
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", config.flussBootstrap());
            Connection connection = ConnectionFactory.createConnection(c);
            Table table = connection.getTable(TablePath.of(config.flussDatabase(), config.intentTable()));
            if (table.getTableInfo().getRowType().getFieldCount() != 22) {
                throw new IllegalStateException("Execution_Intent schema must contain 22 columns");
            }
            return new IntentReader(connection, table, config, forwarder, violationHandler,
                    FlussIntentDedupStore.open(config));
        } catch (Exception e) {
            throw new IllegalStateException("cannot open Execution_Intent", e);
        }
    }

    IntentReader(Connection connection, Table table, GatewayConfig config,
            IntentSink forwarder, Consumer<String> violationHandler,
            IntentDedupStore dedupStore) throws Exception {
        this.connection = connection; this.table = table; this.config = config;
        this.forwarder = forwarder; this.violationHandler = violationHandler;
        this.dedupStore = dedupStore;
        this.dispatcher = new DurableIntentDispatcher(dedupStore);
        this.scanner = table.newScan().createLogScanner();
    }

    /** Subscribe every bucket from offset zero. Replay is intentional after a restart. */
    public void subscribeFromBeginning() {
        for (int bucket = 0; bucket < table.getTableInfo().getNumBuckets(); bucket++) scanner.subscribe(bucket, 0L);
    }

    /** Process one bounded poll. The caller owns the single-writer loop. */
    public int poll(Duration timeout) {
        ScanRecords records = scanner.poll(timeout);
        int accepted = 0;
        for (ScanRecord record : records) {
            try {
                IntentRecord intent = decode(record.getRow(), record.logOffset());
                IntentValidator.validate(intent, config.accountScopeId(), config.executionPartitionId());
                DurableIntentDispatcher.Verdict outcome = dispatcher.classify(
                        intent.instructionId(), intent.requestHash());
                if (outcome == DurableIntentDispatcher.Verdict.HASH_VIOLATION) {
                    violationHandler.accept("instruction hash changed: " + intent.instructionId());
                    continue;
                }
                // Fluss 0.9.1 ScanRecord exposes the log offset but not the
                // bucket. Keep the last observed offset as a process-wide
                // diagnostic until the client exposes bucket metadata.
                lastOffsets.put(-1, record.logOffset());
                if (outcome == DurableIntentDispatcher.Verdict.FIRST) {
                    IntentSink.Result result;
                    try {
                        result = forwarder.forward(intent);
                    } catch (Exception deferred) {
                        LOG.warn("intent handoff deferred (instruction_id={}): {}",
                                intent.instructionId(), deferred.getMessage());
                        continue;
                    }
                    if (result == IntentSink.Result.FORWARDED) {
                        try {
                            dispatcher.committed(intent.instructionId(), intent.requestHash(),
                                    record.logOffset());
                            accepted++;
                        } catch (Exception recursive) {
                            // A durable-commit failure means we cannot prove the
                            // handoff is idempotent; fail closed rather than
                            // silently risk a duplicate side effect on replay.
                            violationHandler.accept(
                                    "durable intent dedup commit failed: "
                                            + intent.instructionId() + " — " + recursive.getMessage());
                        }
                    }
                }
            } catch (RuntimeException e) {
                violationHandler.accept("invalid Execution_Intent at offset " + record.logOffset() + ": " + e.getMessage());
            }
        }
        return accepted;
    }

    public Map<Integer, Long> lastOffsets() { return Map.copyOf(lastOffsets); }
    public static IntentRecord decode(InternalRow r, long offset) {
        if (r == null) throw new IllegalArgumentException("null intent row");
        return new IntentRecord(text(r, 0), text(r, 1), text(r, 2), text(r, 3), text(r, 4),
                r.getLong(5), text(r, 6), text(r, 7), text(r, 8), r.getLong(9), text(r, 10),
                nullableLong(r, 11), text(r, 12), text(r, 13), text(r, 14), text(r, 15), text(r, 16),
                r.getLong(17), nullableLong(r, 18), text(r, 19), nullableText(r, 20), text(r, 21), offset);
    }
    private static String text(InternalRow r, int i) { return r.isNullAt(i) ? "" : r.getString(i).toString(); }
    private static String nullableText(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getString(i).toString(); }
    private static Long nullableLong(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getLong(i); }

    @Override public void close() throws Exception {
        scanner.close(); table.close(); connection.close();
        try { dedupStore.close(); } catch (Exception ignored) { }
    }
}
