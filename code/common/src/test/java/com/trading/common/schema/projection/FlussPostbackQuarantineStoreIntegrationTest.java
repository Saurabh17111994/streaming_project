package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * WP-4 step 3 (T1) live quarantine-write drill, env-gated by {@code FLUSS_BOOTSTRAP}: appends a
 * {@link QuarantinedPostback} through the REAL {@link FlussPostbackQuarantineStore} into a scratch
 * Postback_Quarantine LOG table, then reads the log back via {@link LogScanner} to prove the row
 * durably landed in Fluss.
 */
@Tag("fluss")
class FlussPostbackQuarantineStoreIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final int BUCKETS = 8;

    @Test
    @DisplayName("WP-4 T1: FlussPostbackQuarantineStore appends a durable quarantine LOG row, read back via log scan")
    void appendedQuarantineRowSurvivesAndIsReadableFromFlussLog() throws Exception {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        Assumptions.assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live WP-4 T1 quarantine-store evidence");

        String db = "quar_" + Long.toHexString(System.nanoTime());
        Connection conn = null;
        Admin admin = null;
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", bootstrap);
            conn = ConnectionFactory.createConnection(c);
            admin = conn.getAdmin();
            admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            createPostbackQuarantine(admin, db);

            QuarantinedPostback q = new QuarantinedPostback(
                    "quar-1", "pb-1", QuarantineReason.AMBIGUOUS_CORRELATION,
                    new byte[]{4, 5, 6}, "ph-1", "broker-1", "instr-1", "attempt-1",
                    "OPEN", null, 1_700_000_000_000L, null, "2");
            try (FlussPostbackQuarantineStore store =
                         FlussPostbackQuarantineStore.open(bootstrap, db, "Postback_Quarantine", TIMEOUT)) {
                store.append(q);
            }

            // Read the appended row back from Fluss's LOG via a scanner (durability proof).
            Table t = conn.getTable(TablePath.of(db, "Postback_Quarantine"));
            boolean found = false;
            try (LogScanner scanner = t.newScan().createLogScanner()) {
                for (int b = 0; b < BUCKETS; b++) scanner.subscribeFromBeginning(b);
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline && !found) {
                    ScanRecords recs = scanner.poll(Duration.ofMillis(250));
                    for (ScanRecord sr : recs) {
                        InternalRow row = sr.getRow();
                        if (row != null && row.getString(0) != null
                                && "quar-1".equals(row.getString(0).toString())) {
                            assertThat(row.getString(2).toString()).isEqualTo("AMBIGUOUS_CORRELATION");
                            assertThat(row.getString(8).toString()).isEqualTo("OPEN");
                            found = true;
                            break;
                        }
                    }
                }
            }
            assertThat(found).as("quarantine row 'quar-1' readable from Fluss log").isTrue();
        } finally {
            if (admin != null) {
                try { admin.dropDatabase(db, false, false).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); }
                catch (Exception ignored) { }
            }
            if (conn != null) conn.close();
        }
    }

    private static void createPostbackQuarantine(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("quarantine_id", DataTypes.STRING())
                .column("postback_event_id", DataTypes.STRING())
                .column("reason", DataTypes.STRING())
                .column("original_payload", DataTypes.BYTES())
                .column("payload_hash", DataTypes.STRING())
                .column("broker_order_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("correlation_attempt", DataTypes.STRING())
                .column("disposition", DataTypes.STRING())
                .column("disposition_reason", DataTypes.STRING())
                .column("quarantined_ts", DataTypes.BIGINT())
                .column("disposition_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(s)
                .distributedBy(BUCKETS, "quarantine_id")
                .build();
        admin.createTable(TablePath.of(db, "Postback_Quarantine"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
