package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DEC-038 SIG-STATE-003 (fail-closed startup preflight) — the dedup-table
 * half of the pending row in {@code docs/08_implementation/04-signal-job.md}:
 * {@code SignalJob.preflightTableContracts} must reject the
 * {@code fingerprint_dedup} state table when it is MISSING or
 * schema-INCOMPATIBLE — the job fails closed at startup, never treating the
 * dedup set as empty and never building a graph that would silently replay
 * with an empty set (SIG-STATE-003).
 *
 * <p>The preflight ALREADY validates the dedup table
 * ({@link TableContractValidator#validateFingerprintDedupTable}, ALWAYS-ON
 * in {@link SignalJob#preflightTableContracts}); this test proves the
 * fail-closed behavior LIVE against the dev Fluss cluster with scratch
 * tables: a correct DDL-24-shaped dedup table passes, a missing table and a
 * schema-drifted target both fail with {@link IllegalStateException} (the
 * validator's {@link TableContractValidator.ContractViolation} extends it,
 * so both cases assert the same type, matching the P6.2 pattern). The candle
 * / signal-LOG / signal-current legs are exercised with valid scratch tables
 * so the failure is isolated to the dedup leg. The runtime-unavailability
 * half (lookup failure mid-run fails the task) is already pinned by the unit
 * test {@code FingerprintDedupFunctionTest.rehydrationFailureFailsClosedAndIsCounted}.
 *
 * <p>Gate: {@code COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT=true} — skipped in the
 * normal suite. Live dev cluster ({@code FLUSS_BOOTSTRAP}, default
 * {@code localhost:9123}); skips when unreachable. Host-runnable (no
 * MiniCluster, no Flink). Run:
 * {@code COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SigState003FailClosedPreflightIntegrationTest}
 */
@Tag("integration")
@DisplayName("DEC-038 SIG-STATE-003: preflight fails closed on missing/incompatible fingerprint_dedup (live Fluss)")
class SigState003FailClosedPreflightIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SigState003FailClosedPreflightIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;
    private static String suffix;

    @BeforeAll
    static void connect() {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT", "false")),
                "Skipping — set COMPUTE_INT_TEST_SIG_STATE_PREFLIGHT=true");
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        suffix = String.valueOf(System.nanoTime());
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            LOG.info("sig-003: connected to {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("sig-003: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            ScratchTables.dropCreated(admin, TIMEOUT);
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /** Base env: valid scratch candle/signal tables + the given dedup target. */
    private static Map<String, String> envFor(String dedupTable) {
        Map<String, String> e = new HashMap<>();
        e.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        e.put("FLUSS_DATABASE", "default");
        e.put("RAW_TABLE", "raw_table_1");
        e.put("CANDLE_TABLE", "p6_" + suffix + "_cand");
        e.put("SIGNAL_CANDIDATES_TABLE", "p6_" + suffix + "_sig");
        e.put("SIGNAL_CURRENT_TABLE", "p6_" + suffix + "_cur");
        e.put("DEDUP_STATE_TABLE", dedupTable);
        e.put("DEDUP_TTL_MS", "300000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        e.put("ALLOW_FULL_REPLAY", "true");
        return e;
    }

    @Test
    @DisplayName("preflight passes a DDL-24-shaped dedup table; fails closed on missing + schema drift")
    void dedupPreflightFailClosed() throws Exception {
        // Valid scratch candle KV, signal LOG, signal current KV (non-dedup legs).
        ScratchTables.create(connection, admin, "p6_" + suffix + "_cand",
                ScratchTables.candleSchema(), java.util.List.of("instrument_token", "window_start"),
                16, "candle KV", TIMEOUT);
        ScratchTables.create(connection, admin, "p6_" + suffix + "_sig",
                ScratchTables.signalLogSchema(), null, 16, "signal LOG", TIMEOUT);
        ScratchTables.create(connection, admin, "p6_" + suffix + "_cur",
                ScratchTables.signalCurrentSchema(), java.util.List.of("instrument_token"), 16,
                "signal current KV", TIMEOUT);

        // ── Happy path: exact DDL-24 shape passes the preflight ───────────
        String dedupOk = "p6_" + suffix + "_dedup_ok";
        ScratchTables.createDedup(connection, admin, dedupOk, TIMEOUT);
        TableInfo okInfo = admin.getTableInfo(TablePath.of("default", dedupOk))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("sig-003: dedup table {} (id={}, buckets={}, PK={}, bucketKeys={})",
                dedupOk, okInfo.getTableId(), okInfo.getNumBuckets(),
                okInfo.getPrimaryKeys(), okInfo.getBucketKeys());
        assertDoesNotThrow(() -> SignalJob.preflightTableContracts(
                        SignalJobConfig.from(envFor(dedupOk))),
                "a DDL-24-shaped fingerprint_dedup must pass the startup preflight");

        // ── Missing: nonexistent dedup table must fail closed ─────────────
        String dedupMissing = "p6_" + suffix + "_dedup_missing";
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                        SignalJobConfig.from(envFor(dedupMissing))),
                "a missing fingerprint_dedup table must fail the startup preflight "
                        + "(SIG-STATE-003 — never an empty dedup set)");

        // ── Schema-incompatible: a dedup table missing its schema_version
        //    column (5-col instead of the pinned 6-col v1) must fail closed ──
        String dedupDrift = "p6_" + suffix + "_dedup_drift";
        org.apache.fluss.metadata.Schema driftSchema = org.apache.fluss.metadata.Schema.newBuilder()
                .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
                .column("fingerprint_version", org.apache.fluss.types.DataTypes.STRING())
                .column("event_fingerprint", org.apache.fluss.types.DataTypes.STRING())
                .column("first_seen_ms", org.apache.fluss.types.DataTypes.BIGINT())
                .column("expiry_ms", org.apache.fluss.types.DataTypes.BIGINT())
                .primaryKey("instrument_token", "fingerprint_version", "event_fingerprint")
                .build();
        org.apache.fluss.metadata.TableDescriptor td = org.apache.fluss.metadata.TableDescriptor.builder()
                .schema(driftSchema)
                .distributedBy(16, "instrument_token")
                .property("table.kv.format-version", "2")
                .build();
        admin.createTable(TablePath.of("default", dedupDrift), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        ScratchTables.rememberCreated(dedupDrift);
        assertThrows(IllegalStateException.class, () -> SignalJob.preflightTableContracts(
                        SignalJobConfig.from(envFor(dedupDrift))),
                "a schema-drifted fingerprint_dedup (5-col vs pinned 6-col v1) must fail the "
                        + "startup preflight (STATE-COMPAT-001 / SIG-STATE-003)");

        LOG.info("sig-003: PASS — dedup preflight fail-closed on missing + incompatible, "
                + "accepts the exact DDL-24 shape");
    }
}
