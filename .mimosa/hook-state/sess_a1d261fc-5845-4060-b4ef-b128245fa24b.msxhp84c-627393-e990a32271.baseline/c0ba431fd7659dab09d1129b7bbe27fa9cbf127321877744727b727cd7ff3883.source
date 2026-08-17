import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * ING-TCP-001 count-based losslessness: per-token row counts from the Fluss
 * sink side. raw_table_1 is lake-enabled LOG -> MUST use LogScanner
 * (subscribeFromBeginning; plain BatchScanner caps at the first segment end,
 * p3-2 finding). ingestion_quarantine is plain LOG -> BatchScanner is fine.
 *
 * Columns: raw_table_1 instrument_token = 4; ingestion_quarantine = 2.
 * Env: FLUSS_BOOTSTRAP, FLUSS_RAW_TABLE (default raw_table_1),
 *      FLUSS_QUARANTINE_TABLE (default ingestion_quarantine).
 * Output: per-token RAW= and QUAR= counts, then TOKEN TOTAL=, and a final
 * RECONCILE_TOTAL= (raw+quar) for comparison against the bridge's
 * arrow-tick-counts total.
 */
public final class TokenCountReconcile {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        String rawTable = System.getenv().getOrDefault("FLUSS_RAW_TABLE", "raw_table_1");
        String quarTable = System.getenv().getOrDefault("FLUSS_QUARANTINE_TABLE", "ingestion_quarantine");
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Map<Long, long[]> counts = new TreeMap<>();
            long[] totals = {0, 0};
            scanLog(connection, rawTable, 4, counts, totals, 0);
            scanBatch(connection, quarTable, 2, counts, totals, 1);
            for (Map.Entry<Long, long[]> e : counts.entrySet()) {
                System.out.printf("TOKEN %d RAW=%d QUAR=%d TOTAL=%d%n",
                        e.getKey(), e.getValue()[0], e.getValue()[1],
                        e.getValue()[0] + e.getValue()[1]);
            }
            System.out.println("RECONCILE_RAW_TOTAL=" + totals[0]);
            System.out.println("RECONCILE_QUAR_TOTAL=" + totals[1]);
            System.out.println("RECONCILE_TOTAL=" + (totals[0] + totals[1]));
        }
    }

    /** LogScanner per-bucket full scan (lake-enabled LOG correctness). */
    private static void scanLog(Connection connection, String tableName, int tokenIdx,
                                Map<Long, long[]> counts, long[] totals, int slot)
            throws Exception {
        TablePath path = TablePath.of("default", tableName);
        Table table = connection.getTable(path);
        TableInfo info = connection.getAdmin().getTableInfo(path).join();
        long rows = 0;
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                scanner.subscribeFromBeginning(b);
                long bucketRows = 0;
                int emptyPolls = 0;
                while (emptyPolls < 3) {
                    ScanRecords recs = scanner.poll(Duration.ofMillis(500));
                    int c = recs.count();
                    if (c == 0) {
                        emptyPolls++;
                        continue;
                    }
                    bucketRows += c;
                    emptyPolls = 0;
                    for (ScanRecord rec : recs) {
                        InternalRow row = rec.getRow();
                        long token = row.isNullAt(tokenIdx) ? -1 : row.getLong(tokenIdx);
                        long[] v = counts.computeIfAbsent(token, k -> new long[2]);
                        v[slot]++;
                    }
                }
                scanner.unsubscribe(b);
                System.out.println("RAW_BUCKET_" + b + "=" + bucketRows);
                rows += bucketRows;
            }
        }
        totals[0] = rows;
        System.out.println("RAW_TOTAL=" + rows);
    }

    /** BatchScanner per-bucket scan (plain LOG table). */
    private static void scanBatch(Connection connection, String tableName, int tokenIdx,
                                  Map<Long, long[]> counts, long[] totals, int slot)
            throws Exception {
        TablePath path = TablePath.of("default", tableName);
        Table table = connection.getTable(path);
        TableInfo info = connection.getAdmin().getTableInfo(path).join();
        long rows = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            long bucketRows = 0;
            try (BatchScanner scanner =
                         table.newScan().limit(1_000_000_000).createBatchScanner(tb);
                 CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(30_000))) {
                while (it.hasNext()) {
                    InternalRow row = it.next();
                    bucketRows++;
                    long token = row.isNullAt(tokenIdx) ? -1 : row.getLong(tokenIdx);
                    long[] v = counts.computeIfAbsent(token, k -> new long[2]);
                    v[slot]++;
                }
            }
            System.out.println("QUAR_BUCKET_" + b + "=" + bucketRows);
            rows += bucketRows;
        }
        totals[1] = rows;
        System.out.println("QUAR_TOTAL=" + rows);
    }
}
