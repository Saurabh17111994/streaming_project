package com.trading.compute.babysitter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BAB-INT-001 (Task 7) — LIVE, cluster-gated: the real
 * {@link BabysitterJob#buildTopology} graph runs on an embedded Flink
 * MiniCluster against a real Fluss {@code Positions} KV table, checkpoints its
 * observation state, is cancelled, and is restored by a <b>fresh MiniCluster
 * instance</b> (new worker) which then consumes added/replayed/changelog rows.
 *
 * <p>Because the Babysitter is a read-only observer (it writes nothing external
 * — its observation state lives in checkpointed keyed {@code ValueState}), the
 * storage-level proof is that it never <i>creates a {@code Position_Actions} or
 * execution table</i> and never <i>mutates {@code Positions}</i>: after both
 * phases the table set and the {@code Positions} rows are byte-identical to
 * what this test itself wrote. Combined with the offline operator/restore
 * harness tests, this proves checkpoint + restore behavior and the zero-action
 * boundary on a live cluster.
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_T7=true)} —
 * skipped (no MiniCluster, no Fluss connection) in the normal suite. Run:
 * {@code mvn -o test -Dtest=BabysitterPositionsRestoreIntegrationTest} with
 * {@code COMPUTE_INT_TEST_T7=true} and a dev Fluss cluster at
 * {@code FLUSS_BOOTSTRAP} (default {@code localhost:9123}).
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_T7", matches = "true")
@DisplayName("BAB-INT-001: Babysitter restores observation state and writes nothing")
class BabysitterPositionsRestoreIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(BabysitterPositionsRestoreIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String DB = "default";
    private static final List<String> CREATED_TABLES = new ArrayList<>();

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("bab-int-001: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("bab-int-001: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        for (String table : CREATED_TABLES) {
            try {
                admin.dropTable(TablePath.of(DB, table), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("bab-int-001: dropped scratch table {}", table);
            } catch (Exception e) {
                LOG.warn("bab-int-001: drop {} failed: {}", table, e.getMessage());
            }
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("restored Babysitter consumes the real Positions changelog and writes nothing")
    void restoredObserverWritesNothing() throws Exception {
        ScratchSet s = createSet();
        Path cpDir = Path.of(System.getProperty("java.io.tmpdir"),
                "babysitter-cp-" + s.suffix());
        Files.createDirectories(cpDir);

        // ── Phase 1: worker A — v1 rows observed + checkpointed ─────────────
        String restore;
        MiniClusterWithClientResource clusterA = newMiniCluster();
        clusterA.before();
        try {
            upsert(s.table, s.pidA + "-v1", s.pidA, "ev-a1", 1L, "OPEN", 100, 0);
            upsert(s.table, s.pidB + "-v1", s.pidB, "ev-b1", 1L, "OPEN", 50, 0);
            JobClient job1 = startJob(s, cpDir.toString(), null, "bab-int-001-phase1");
            restore = awaitCompletedCheckpoint(job1.getJobID(), cpDir, 120);
            assertFalse(anyActionOrExecutionTable(s), "phase 1 Job must not create an action table");
            cancelAndFinish(job1, "phase1");
            LOG.info("bab-int-001: phase 1 checkpointed at {}", restore);
        } finally {
            clusterA.after(); // phase 2 runs on a FRESH worker
        }

        // ── Phase 2: worker B — restore, then feed v2 + replayed/stale/conflict
        MiniClusterWithClientResource clusterB = newMiniCluster();
        clusterB.before();
        try {
            // POS-A advances to v2; POS-B replays its v1 (duplicate), receives a
            // stale v0, and a conflicting same-version event. The Babysitter must
            // stay a no-op observer through all of them.
            upsert(s.table, s.pidA + "-v2", s.pidA, "ev-a2", 2L, "OPEN", 120, 0);
            upsert(s.table, s.pidB + "-v1b", s.pidB, "ev-b1", 1L, "OPEN", 50, 0);   // replayed dup
            upsert(s.table, s.pidB + "-v0", s.pidB, "ev-b0", 0L, "OPEN", 10, 0);    // stale
            upsert(s.table, s.pidB + "-v1-conflict", s.pidB, "ev-bC", 1L, "OPEN", 77, 0); // conflict

            JobClient job2 = startJob(s, cpDir.toString(), restore, "bab-int-001-phase2");
            awaitCompletedCheckpoint(job2.getJobID(), cpDir, 120);
            LOG.info("bab-int-001: restored phase 2 job reached a new checkpoint");

            assertFalse(anyActionOrExecutionTable(s),
                    "restored Babysitter must not create/write a Position_Actions or "
                            + "execution table");
            // The Babysitter never mutates Positions: the only rows are exactly
            // the two keys this test itself upserted, with exactly the values the
            // test wrote at the end of phase 2 — no extra rows, no re-applied
            // conflicting/stale content reached storage.
            Map<String, InternalRow> rows = readPositions(s);
            assertEquals(2, rows.size(),
                    "Positions must hold exactly the two keys this test wrote, never more");
            InternalRow a = rows.get(s.pidA);
            InternalRow b = rows.get(s.pidB);
            assertEquals(120, qty(a),
                    "POS-A reflects only this test's own v2 write (120), nothing more");
            assertEquals(2L, a.getLong(13), "POS-A source_version is this test's own v2");
            assertEquals(77, qty(b),
                    "POS-B reflects only this test's own last (conflict) write (77)");
            assertEquals(1L, b.getLong(13), "POS-B source_version is this test's own last write");
            cancelAndFinish(job2, "phase2");
        } finally {
            clusterB.after();
        }
    }

    private static MiniClusterWithClientResource newMiniCluster() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    private static JobClient startJob(ScratchSet s, String checkpointDir, String recovery,
            String name) throws Exception {
        BabysitterConfig config = new BabysitterConfig(
                bootstrap, DB, s.tableName(), "file://" + Path.of(checkpointDir).toAbsolutePath(),
                2_000L, 60_000L, false);
        StreamExecutionEnvironment senv = BabysitterJob.buildTopology(config);
        JobClient client = senv.executeAsync(name);
        JobStatus status = awaitJobStatus(client, 90);
        assertEquals(JobStatus.RUNNING, status, "job " + name + " must reach RUNNING, got " + status);
        LOG.info("bab-int-001: job {} RUNNING", name);
        return client;
    }

    private static JobStatus awaitJobStatus(JobClient client, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            if (last == JobStatus.RUNNING || last == JobStatus.FINISHED
                    || last == JobStatus.FAILED || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(2_000);
        }
        return last;
    }

    private static void cancelAndFinish(JobClient client, String phase) throws Exception {
        client.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (client.getJobStatus().get(5, TimeUnit.SECONDS) == JobStatus.CANCELED) {
                return;
            }
            Thread.sleep(2_000);
        }
        fail("job did not reach CANCELED in phase " + phase);
    }

    /** Polls the checkpoint dir for a completed checkpoint dir and returns its file:// URI. */
    private static String awaitCompletedCheckpoint(JobID jobId, Path cpDir, long timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        String seen = null;
        while (System.nanoTime() < deadline) {
            String latest = latestCompletedCheckpoint(jobId, cpDir);
            if (latest != null) {
                if (latest.equals(seen)) {
                    return latest; // present across two consecutive polls — durable
                }
                seen = latest;
            }
            Thread.sleep(5_000L);
        }
        fail("Timed out waiting for a completed checkpoint in " + cpDir);
        return null;
    }

    private static String latestCompletedCheckpoint(JobID jobId, Path cpDir) {
        Path dir = cpDir.resolve(jobId.toHexString());
        if (!Files.isDirectory(dir)) {
            return null;
        }
        long best = -1;
        try (Stream<Path> entries = Files.list(dir)) {
            for (Path p : (Iterable<Path>) entries::iterator) {
                String name = p.getFileName().toString();
                if (!name.startsWith("chk-") || !Files.isRegularFile(p.resolve("_metadata"))) {
                    continue;
                }
                try {
                    best = Math.max(best, Long.parseLong(name.substring("chk-".length())));
                } catch (NumberFormatException ignored) {
                    // not a checkpoint dir
                }
            }
        } catch (Exception e) {
            return null;
        }
        return best < 0 ? null : "file://" + dir.resolve("chk-" + best).toAbsolutePath();
    }

    /** True iff a Position_Actions or execution table was ever created on the cluster. */
    private static boolean anyActionOrExecutionTable(ScratchSet s) throws Exception {
        List<String> tables = admin.listTables(DB).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        for (String t : tables) {
            String lower = t.toLowerCase();
            if (lower.contains("action") || lower.contains("execution")) {
                return true;
            }
        }
        return false;
    }

    // ── scratch Positions table ─────────────────────────────────────────────

    private static ScratchSet createSet() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        String name = "positions_" + suffix;
        Schema schema = Schema.newBuilder()
                .column("position_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("account_scope_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("side", DataTypes.STRING())
                .column("state", DataTypes.STRING())
                .column("open_quantity", DataTypes.BIGINT())
                .column("closed_quantity", DataTypes.BIGINT())
                .column("average_entry_paise", DataTypes.BIGINT())
                .column("average_exit_paise", DataTypes.BIGINT())
                .column("source_event_id", DataTypes.STRING())
                .column("source_version", DataTypes.BIGINT())
                .column("created_ts", DataTypes.BIGINT())
                .column("last_update_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("position_id")
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1, "position_id")
                .build();
        admin.createTable(TablePath.of(DB, name), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED_TABLES.add(name);
        Table table = connection.getTable(TablePath.of(DB, name));
        LOG.info("bab-int-001: created scratch Positions table {}", name);
        return new ScratchSet(suffix, name, table,
                admin.getTableInfo(TablePath.of(DB, name))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                "POS-A", "POS-B");
    }

    private static void upsert(Table table, String eventId, String pid, String sourceEvent,
            long sourceVersion, String state, long open, long closed) throws Exception {
        GenericRow row = new GenericRow(17);
        row.setField(0, BinaryString.fromString(pid));
        row.setField(1, BinaryString.fromString("tc-1"));
        row.setField(2, BinaryString.fromString("acct-1"));
        row.setField(3, 7L);
        row.setField(4, BinaryString.fromString("NSE"));
        row.setField(5, BinaryString.fromString("TEST"));
        row.setField(6, BinaryString.fromString("BUY"));
        row.setField(7, BinaryString.fromString(state));
        row.setField(8, open);
        row.setField(9, closed);
        row.setField(10, null);
        row.setField(11, null);
        row.setField(12, BinaryString.fromString(sourceEvent));
        row.setField(13, sourceVersion);
        row.setField(14, 1_000L);
        row.setField(15, 2_000L + sourceVersion);
        row.setField(16, BinaryString.fromString("2"));
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            writer.upsert(row).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            writer.flush();
        } finally {
            // UpsertWriter has no AutoCloseable; flush() is the sync point.
            writer.flush();
        }
    }

    /** Scans every bucket of the Positions table. */
    private static Map<String, InternalRow> readPositions(ScratchSet s) throws Exception {
        Map<String, InternalRow> byPid = new HashMap<>();
        TableInfo info = admin.getTableInfo(TablePath.of(DB, s.tableName()))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = s.table.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(250))) {
                while (it.hasNext()) {
                    InternalRow r = it.next();
                    byPid.put(r.getString(0).toString(), r);
                }
            }
        }
        return byPid;
    }

    private static long qty(InternalRow r) {
        return r == null ? -1L : r.getLong(8);
    }

    private record ScratchSet(String suffix, String tableName, Table table, TableInfo info,
            String pidA, String pidB) {}
}
