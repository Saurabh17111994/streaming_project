package com.trading.ingestion;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

/** Read-only terminal viewer for newly persisted raw ticks. */
public final class TickTableViewer {
    private static final int DEFAULT_LIMIT = 20;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private TickTableViewer() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = env("FLUSS_BOOTSTRAP", "localhost:9123");
        String tableName = env("RAW_TABLE_NAME", "raw_table_1");
        int limit = args.length == 0 ? DEFAULT_LIMIT : Integer.parseInt(args[0]);
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be greater than zero");
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tablePath = TablePath.of("default", tableName);

        try (Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin();
                Table table = connection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            int buckets = table.getTableInfo().getNumBuckets();
            List<Integer> bucketIds = new ArrayList<>();
            for (int bucket = 0; bucket < buckets; bucket++) {
                bucketIds.add(bucket);
            }
            subscribeFromLatest(admin, scanner, tablePath, bucketIds);

            Deque<ScanRecord> latest = new ArrayDeque<>(limit);
            System.out.println("Fluss table: " + tablePath);
            System.out.println("Following new rows; showing the latest " + limit + ". Press Ctrl+C to stop.");
            while (true) {
                ScanRecords records = scanner.poll(POLL_TIMEOUT);
                for (ScanRecord record : records) {
                    if (latest.size() == limit) {
                        latest.removeFirst();
                    }
                    latest.addLast(record);
                }
                if (records.count() > 0) {
                    printLatest(latest);
                }
            }
        }
    }

    private static void subscribeFromLatest(
            Admin admin, LogScanner scanner, TablePath tablePath, Collection<Integer> buckets)
            throws Exception {
        ListOffsetsResult offsets = admin.listOffsets(tablePath, buckets, new LatestSpec());
        offsets.all().get().forEach(scanner::subscribe);
    }

    private static void printLatest(Deque<ScanRecord> records) {
        System.out.print("\033[H\033[2J");
        System.out.flush();
        System.out.println("Latest persisted ticks (" + records.size() + ")");
        System.out.println("event_time | token | exchange | symbol | price | qty | tick_type | validity | log_offset | storage_time");
        System.out.println("-----------+-------+----------+--------+-------+-----+-----------+----------+------------+-------------");
        records.forEach(TickTableViewer::print);
        System.out.println("\nUpdated: " + Instant.now() + " | Press Ctrl+C to stop.");
    }

    private static void print(ScanRecord record) {
        InternalRow row = record.getRow();
        System.out.printf(
                "%s | %d | %s | %s | %.2f | %d | %s | %s | %d | %s%n",
                Instant.ofEpochMilli(row.getLong(11)),
                row.getLong(4),
                text(row, 5),
                text(row, 6),
                row.getLong(15) / 100.0,
                row.getLong(16),
                text(row, 14),
                text(row, 25),
                record.logOffset(),
                record.timestamp() < 0 ? "" : Instant.ofEpochMilli(record.timestamp()));
    }

    private static String text(InternalRow row, int index) {
        if (row.isNullAt(index)) {
            return "";
        }
        BinaryString value = row.getString(index);
        return value == null ? "" : value.toString();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
