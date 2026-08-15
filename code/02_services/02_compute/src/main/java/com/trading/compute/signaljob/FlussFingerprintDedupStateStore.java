package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * {@link FingerprintDedupStateStore} backed by the {@code fingerprint_dedup}
 * KV table via the Fluss raw client (DEC-038). The table shape
 * (composite PK {@code (instrument_token, fingerprint_version,
 * event_fingerprint)}, single-field bucket key {@code instrument_token},
 * {@code kv.format-version=2}) is the documented-working COMPAT-FLUSS-005
 * combo for raw-client upsert — same shape as {@code feature_candles_15s}.
 *
 * <p>Bucket for a token is {@code token % bucketNumber} (bucket key =
 * {@code instrument_token}), so scan/delete are scoped to one bucket. Delete
 * on composite keys is evidence-gated (SIG-STATE-001); the mechanism here is
 * the same encoder path the proven upsert uses.
 *
 * <p>Owns its {@link Connection} — {@link #close()} releases it.
 */
public final class FlussFingerprintDedupStateStore implements FingerprintDedupStateStore {

    private static final String KEY_SEP = "|";

    private final Connection connection;
    private final Table table;
    private final TableInfo info;
    private final long timeoutMs;

    private FlussFingerprintDedupStateStore(Connection connection, Table table, TableInfo info,
            long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.info = info;
        this.timeoutMs = timeoutMs;
    }

    /** Open the store against {@code database.dedupTable} on the cluster. */
    public static FlussFingerprintDedupStateStore open(String bootstrap, String database,
            String dedupTable, Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            TablePath path = TablePath.of(database, dedupTable);
            TableInfo info = connection.getAdmin().getTableInfo(path)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Table table = connection.getTable(path);
            return new FlussFingerprintDedupStateStore(connection, table, info, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private static String key(long token, String version, String fingerprint) {
        return token + KEY_SEP + version + KEY_SEP + fingerprint;
    }

    /** Key row for lookup/delete — PK fields in DDL order. */
    private static GenericRow keyRow(long token, String version, String fingerprint) {
        return GenericRow.of(token, BinaryString.fromString(version),
                BinaryString.fromString(fingerprint));
    }

    private int bucketOf(long token) {
        return (int) ((token % info.getNumBuckets() + info.getNumBuckets()) % info.getNumBuckets());
    }

    @Override
    public Lookup lookup(long token, String version, String fingerprint, long nowMs)
            throws Exception {
        // Lookuper is not AutoCloseable in 0.9.1 (same as EodStateStore's);
        // each lookup creates a fresh lookuper that the client reclaims.
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow found = lookuper.lookup(keyRow(token, version, fingerprint))
                .get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow();
        if (found == null) {
            return Lookup.NOT_SEEN;
        }
        long expiryMs = found.getLong(FingerprintDedupTableColumns.EXPIRY_MS);
        return DedupExpiry.isExpired(expiryMs, nowMs) ? Lookup.SEEN_EXPIRED : Lookup.SEEN_LIVE;
    }

    @Override
    public void putFirstSeen(long token, String version, String fingerprint,
            long firstSeenMs, long expiryMs) throws Exception {
        Object[] values = new Object[FingerprintDedupTableColumns.FIELD_COUNT];
        values[FingerprintDedupTableColumns.INSTRUMENT_TOKEN] = token;
        values[FingerprintDedupTableColumns.FINGERPRINT_VERSION] =
                BinaryString.fromString(version);
        values[FingerprintDedupTableColumns.EVENT_FINGERPRINT] =
                BinaryString.fromString(fingerprint);
        values[FingerprintDedupTableColumns.FIRST_SEEN_MS] = firstSeenMs;
        values[FingerprintDedupTableColumns.EXPIRY_MS] = expiryMs;
        values[FingerprintDedupTableColumns.SCHEMA_VERSION] =
                BinaryString.fromString(FingerprintDedupTableColumns.SCHEMA_VERSION_V1);
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            writer.upsert(GenericRow.of(values)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }

    @Override
    public List<DedupExpiry.CleanupCandidate> scanExpired(long token, long nowMs,
            int maxBatchSize) throws Exception {
        TableBucket tb = new TableBucket(info.getTableId(), bucketOf(token));
        List<DedupExpiry.CleanupCandidate> candidates = new ArrayList<>();
        try (BatchScanner scanner = table.newScan()
                     .limit(Integer.MAX_VALUE)
                     .createBatchScanner(tb);
             CloseableIterator<InternalRow> it =
                     scanner.pollBatch(Duration.ofMillis(250))) {
            while (it.hasNext()) {
                InternalRow row = it.next();
                long t = row.getLong(FingerprintDedupTableColumns.INSTRUMENT_TOKEN);
                String v = row.getString(FingerprintDedupTableColumns.FINGERPRINT_VERSION)
                        .toString();
                String f = row.getString(FingerprintDedupTableColumns.EVENT_FINGERPRINT)
                        .toString();
                long firstSeen = row.getLong(FingerprintDedupTableColumns.FIRST_SEEN_MS);
                long expiry = row.getLong(FingerprintDedupTableColumns.EXPIRY_MS);
                candidates.add(new DedupExpiry.CleanupCandidate(key(t, v, f), firstSeen, expiry));
            }
        }
        return DedupExpiry.selectBatch(candidates, nowMs, maxBatchSize);
    }

    @Override
    public void delete(List<DedupExpiry.CleanupCandidate> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            for (DedupExpiry.CleanupCandidate c : batch) {
                String[] parts = c.key().split("\\" + KEY_SEP, 3);
                long token = Long.parseLong(parts[0]);
                String version = parts[1];
                String fingerprint = parts[2];
                writer.delete(keyRow(token, version, fingerprint))
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } finally {
            writer.flush();
        }
    }
}
