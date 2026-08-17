package com.trading.common.schema.eod;

import com.trading.common.schema.EodControllerState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * {@link EodStateStore} backed by the {@code eod_offload_state} KV table via
 * the Fluss raw client (SCH-23). The controller is a plain-JVM runner, so the
 * state table is single-field-PK by design (COMPAT-FLUSS-005: the raw client
 * cannot upsert composite-PK KV tables in Fluss 0.9.1) and all I/O here is
 * raw-client: current-state reads via per-bucket {@link BatchScanner} (folded
 * by {@code record_id} — last-write-wins, so re-runs converge), writes via
 * {@link UpsertWriter}.
 *
 * <p>Owns its {@link Connection} — {@link #close()} releases it.
 */
public final class FlussEodStateStore implements EodStateStore, AutoCloseable {

    private final Connection connection;
    private final Table table;
    private final TableInfo info;
    private final long timeoutMs;

    private FlussEodStateStore(Connection connection, Table table, TableInfo info,
            long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.info = info;
        this.timeoutMs = timeoutMs;
    }

    /** Open the store against {@code database.stateTable} on the cluster. */
    public static FlussEodStateStore open(String bootstrap, String database, String stateTable,
            Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            TablePath path = TablePath.of(database, stateTable);
            TableInfo info = connection.getAdmin().getTableInfo(path)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Table table = connection.getTable(path);
            return new FlussEodStateStore(connection, table, info, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public List<EodOffloadRecord> readAll() throws Exception {
        Map<String, InternalRow> rows = new LinkedHashMap<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    InternalRow row = it.next();
                    rows.put(row.getString(EodOffloadStateColumns.RECORD_ID).toString(), row);
                }
            }
        }
        List<EodOffloadRecord> out = new ArrayList<>();
        for (InternalRow row : rows.values()) {
            String recordId = row.getString(EodOffloadStateColumns.RECORD_ID).toString();
            if (EodOffloadStateColumns.LEASE_RECORD_ID.equals(recordId)) {
                continue; // the lease row is not an offload record
            }
            out.add(toRecord(row));
        }
        return out;
    }

    @Override
    public void upsert(EodOffloadRecord record) throws Exception {
        Object[] values = new Object[EodOffloadStateColumns.FIELD_COUNT];
        values[EodOffloadStateColumns.RECORD_ID] = BinaryString.fromString(
                EodOffloadStateColumns.recordId(record.tradingDate(), record.tableName()));
        values[EodOffloadStateColumns.TRADING_DATE] = BinaryString.fromString(record.tradingDate());
        values[EodOffloadStateColumns.TABLE_NAME] = BinaryString.fromString(record.tableName());
        values[EodOffloadStateColumns.SCHEMA_VERSION] = BinaryString.fromString(record.schemaVersion());
        values[EodOffloadStateColumns.SOURCE_OFFSET_START] = record.sourceOffsetStart();
        values[EodOffloadStateColumns.SOURCE_OFFSET_END] = record.sourceOffsetEnd();
        values[EodOffloadStateColumns.ROW_COUNT] = record.rowCount();
        values[EodOffloadStateColumns.BYTE_COUNT] = record.byteCount();
        values[EodOffloadStateColumns.SOURCE_HASH] = BinaryString.fromString(record.sourceHash());
        values[EodOffloadStateColumns.TARGET_HASH] = BinaryString.fromString(record.targetHash());
        values[EodOffloadStateColumns.ICEBERG_SNAPSHOT_ID] =
                BinaryString.fromString(record.icebergSnapshotId());
        values[EodOffloadStateColumns.STATE] = BinaryString.fromString(record.state().name());
        values[EodOffloadStateColumns.RETRY_COUNT] = record.retryCount();
        values[EodOffloadStateColumns.NEXT_RETRY_AT_MS] = record.nextRetryAtMs();
        values[EodOffloadStateColumns.EARLIEST_ALLOWED_SOURCE_EXPIRY_MS] =
                record.earliestAllowedSourceExpiryMs();
        values[EodOffloadStateColumns.UPDATED_AT_MS] = record.updatedAtMs();
        values[EodOffloadStateColumns.STATE_SCHEMA_VERSION] =
                BinaryString.fromString(EodOffloadStateColumns.STATE_SCHEMA_VERSION_V1);
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            writer.upsert(GenericRow.of(values)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }

    @Override
    public Lease acquireLease(String token, long nowMs, long leaseTtlMs) throws Exception {
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow found = lookuper.lookup(GenericRow.of(BinaryString.fromString(
                        EodOffloadStateColumns.LEASE_RECORD_ID)))
                .get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow();
        if (found != null) {
            long expiry = found.getLong(EodOffloadStateColumns.SOURCE_OFFSET_START);
            String holder = found.getString(EodOffloadStateColumns.SOURCE_HASH).toString();
            if (expiry >= nowMs && !token.equals(holder)) {
                // held by another, unexpired — refuse (best-effort fencing)
                return new Lease(holder, expiry,
                        found.getLong(EodOffloadStateColumns.UPDATED_AT_MS));
            }
        }
        // absent, expired, or our own token — acquire/refresh
        upsertLease(token, nowMs + leaseTtlMs, nowMs);
        return new Lease(token, nowMs + leaseTtlMs, nowMs);
    }

    private void upsertLease(String token, long expiryMs, long acquiredAtMs) throws Exception {
        Object[] values = new Object[EodOffloadStateColumns.FIELD_COUNT];
        values[EodOffloadStateColumns.RECORD_ID] =
                BinaryString.fromString(EodOffloadStateColumns.LEASE_RECORD_ID);
        values[EodOffloadStateColumns.TRADING_DATE] =
                BinaryString.fromString(EodOffloadStateColumns.LEASE_TRADING_DATE);
        values[EodOffloadStateColumns.TABLE_NAME] =
                BinaryString.fromString(EodOffloadStateColumns.LEASE_TABLE_NAME);
        values[EodOffloadStateColumns.SCHEMA_VERSION] =
                BinaryString.fromString(EodOffloadStateColumns.STATE_SCHEMA_VERSION_V1);
        values[EodOffloadStateColumns.SOURCE_OFFSET_START] = expiryMs; // lease expiry
        values[EodOffloadStateColumns.SOURCE_OFFSET_END] = 0L;
        values[EodOffloadStateColumns.ROW_COUNT] = 0L;
        values[EodOffloadStateColumns.BYTE_COUNT] = 0L;
        values[EodOffloadStateColumns.SOURCE_HASH] = BinaryString.fromString(token); // lease token
        values[EodOffloadStateColumns.TARGET_HASH] = BinaryString.fromString("");
        values[EodOffloadStateColumns.ICEBERG_SNAPSHOT_ID] = BinaryString.fromString("LEASE");
        values[EodOffloadStateColumns.STATE] = BinaryString.fromString("ACTIVE");
        values[EodOffloadStateColumns.RETRY_COUNT] = 0;
        values[EodOffloadStateColumns.NEXT_RETRY_AT_MS] = 0L;
        values[EodOffloadStateColumns.EARLIEST_ALLOWED_SOURCE_EXPIRY_MS] = Long.MAX_VALUE;
        values[EodOffloadStateColumns.UPDATED_AT_MS] = acquiredAtMs;
        values[EodOffloadStateColumns.STATE_SCHEMA_VERSION] =
                BinaryString.fromString(EodOffloadStateColumns.STATE_SCHEMA_VERSION_V1);
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            writer.upsert(GenericRow.of(values)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }

    private static EodOffloadRecord toRecord(InternalRow r) {
        return new EodOffloadRecord(
                r.getString(EodOffloadStateColumns.TRADING_DATE).toString(),
                r.getString(EodOffloadStateColumns.TABLE_NAME).toString(),
                r.getString(EodOffloadStateColumns.SCHEMA_VERSION).toString(),
                r.getLong(EodOffloadStateColumns.SOURCE_OFFSET_START),
                r.getLong(EodOffloadStateColumns.SOURCE_OFFSET_END),
                r.getLong(EodOffloadStateColumns.ROW_COUNT),
                r.getLong(EodOffloadStateColumns.BYTE_COUNT),
                r.getString(EodOffloadStateColumns.SOURCE_HASH).toString(),
                r.getString(EodOffloadStateColumns.TARGET_HASH).toString(),
                r.getString(EodOffloadStateColumns.ICEBERG_SNAPSHOT_ID).toString(),
                EodControllerState.valueOf(r.getString(EodOffloadStateColumns.STATE).toString()),
                r.getInt(EodOffloadStateColumns.RETRY_COUNT),
                r.getLong(EodOffloadStateColumns.NEXT_RETRY_AT_MS),
                r.getLong(EodOffloadStateColumns.EARLIEST_ALLOWED_SOURCE_EXPIRY_MS),
                r.getLong(EodOffloadStateColumns.UPDATED_AT_MS));
    }
}
