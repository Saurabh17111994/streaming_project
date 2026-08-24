package com.trading.common.schema.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CHG-100 live half: the step-7 smoke runs against a scratch twin (never the
 * real table) and the fixture sweep detects/repairs rows left by the
 * pre-CHG-100 smoke path.
 *
 * <p>Requires {@code FLUSS_BOOTSTRAP} (same gate as COMPAT-FLUSS-001). The
 * unit half lives in {@link DdlSmokeTwinSweepUnitTest} so the class-level
 * gate never suppresses the cluster-free tests.
 */
@Tag("integration")
@DisplayName("CHG-100: twin smoke + fixture sweep")
class DdlSmokeTwinSweepTest {

    private static final Logger LOG = LoggerFactory.getLogger(DdlSmokeTwinSweepTest.class);
    private static final java.time.Duration TIMEOUT = java.time.Duration.ofSeconds(30);

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;
    private static final List<String> CREATED = new ArrayList<>();

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run the CHG-100 integration tests");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            LOG.warn("chg-100: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            for (String name : CREATED) {
                try {
                    admin.dropTable(TablePath.of("default", name), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("chg-100: dropped {}", name);
                } catch (Exception e) {
                    LOG.warn("chg-100: drop {} failed: {}", name, e.getMessage());
                }
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    // ── live cluster tests ─────────────────────────────────────────────────

    @Test
    @DisplayName("full apply leaves zero fixtures and zero twins behind")
    void applySmokeRunsOnTwinAndLeavesNoFixtures() throws Exception {
        assumeTrue(admin != null, "no live cluster");
        Path ddlDir = resolveDdlDir();
        String prefix = "chg100apply" + System.nanoTime() + "_";
        Path evidence = Files.createTempFile("chg100-apply-", ".json");
        int rc = DdlApplyTool.run(new String[] {
                "--ddl-dir", ddlDir.toString(),
                "--bootstrap", bootstrap,
                "--table-prefix", prefix,
                "--evidence-out", evidence.toString(),
                "--ack-limitations", "auto",
        });
        assertTrue(rc == 0 || rc == 6, "apply must PASS (0) or PASS_WITH_LIMITATION (6), got " + rc);
        List<String> live = admin.listTables("default")
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        List<String> leftovers = live.stream()
                .filter(n -> n.startsWith(prefix) || n.startsWith("smoke_twin_"))
                .toList();
        assertTrue(leftovers.isEmpty(),
                "no prefix tables or smoke twins may remain after the apply: " + leftovers);
    }

    @Test
    @DisplayName("sweep detects fixture rows; --sweep-fix-kv deletes KV, reports LOG")
    void sweepDetectsAndFixesKvOnly() throws Exception {
        assumeTrue(admin != null, "no live cluster");
        String logName = "chg100_sweep_log_" + System.nanoTime();
        String kvName = "chg100_sweep_kv_" + System.nanoTime();
        CREATED.add(logName);
        CREATED.add(kvName);

        DdlText.ParsedDdl logDdl = DdlText.parse(
                "CREATE TABLE " + logName + " (\n"
                        + "    a STRING NOT NULL,\n"
                        + "    b BIGINT NOT NULL\n"
                        + ") WITH (\n"
                        + "    'bucket.num' = '1',\n"
                        + "    'bucket.key' = 'a',\n"
                        + "    'table.log.ttl' = '7d'\n"
                        + ")",
                "inline-log");
        DdlText.ParsedDdl kvDdl = DdlText.parse(
                "CREATE TABLE " + kvName + " (\n"
                        + "    a STRING NOT NULL,\n"
                        + "    b BIGINT NOT NULL,\n"
                        + "    PRIMARY KEY (a) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "    'bucket.num' = '1',\n"
                        + "    'bucket.key' = 'a',\n"
                        + "    'table.log.ttl' = '7d'\n"
                        + ")",
                "inline-kv");
        admin.createTable(TablePath.of("default", logName), DdlText.toDescriptor(logDdl), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        admin.createTable(TablePath.of("default", kvName), DdlText.toDescriptor(kvDdl), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Table logTable = connection.getTable(TablePath.of("default", logName));
        Table kvTable = connection.getTable(TablePath.of("default", kvName));

        // Fixture rows (exact pre-CHG-100 signature) + one REAL row per table.
        Object[] fixture = {
                BinaryString.fromString("smoke-0"),
                2L
        };
        AppendWriter logWriter = logTable.newAppend().createWriter();
        try {
            logWriter.append(GenericRow.of(fixture)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            logWriter.append(GenericRow.of(BinaryString.fromString("real_key"), 999L))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            logWriter.flush();
        }
        Object[] kvFixture = {BinaryString.fromString("smoke-0"), 2L};
        Object[] kvReal = {BinaryString.fromString("real_key"), 999L};
        UpsertWriter kvWriter = kvTable.newUpsert().createWriter();
        try {
            kvWriter.upsert(GenericRow.of(kvFixture)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            kvWriter.upsert(GenericRow.of(kvReal)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            kvWriter.flush();
        }

        int rcReport = DdlApplyTool.sweep(connection, admin,
                options(new String[] {"--sweep-table", logName, "--sweep-table", kvName}));
        assertEquals(3, rcReport, "report-only sweep must flag the polluted tables");

        int rcFix = DdlApplyTool.sweep(connection, admin,
                options(new String[] {"--sweep-table", logName, "--sweep-table", kvName,
                        "--sweep-fix-kv"}));
        assertEquals(3, rcFix, "LOG fixture rows are undeletable — exit 3 even after KV fix");

        Lookuper lookuper = kvTable.newLookup().createLookuper();
        assertNull(lookuper.lookup(GenericRow.of(BinaryString.fromString("smoke-0")))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow(),
                "KV fixture key must be deleted by --sweep-fix-kv");
        assertNotNull(lookuper.lookup(GenericRow.of(BinaryString.fromString("real_key")))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow(),
                "the real KV row must survive the sweep");

        // After dropping the LOG table, the same scoped sweep is CLEAN.
        admin.dropTable(TablePath.of("default", logName), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED.remove(logName);
        int rcClean = DdlApplyTool.sweep(connection, admin,
                options(new String[] {"--sweep-table", kvName, "--sweep-fix-kv"}));
        assertEquals(0, rcClean, "sweep is CLEAN once no fixture rows remain");
    }

    private static DdlApplyTool.Options options(String[] args) {
        List<String> full = new ArrayList<>();
        full.add("--ddl-dir");
        full.add(".");
        full.add("--bootstrap");
        full.add(bootstrap == null ? "localhost:9123" : bootstrap);
        full.addAll(List.of(args));
        return DdlApplyTool.Options.parse(full.toArray(new String[0]));
    }

    /** Locate {@code code/01_platform/02_sql/ddl} by walking up from the working directory. */
    private static Path resolveDdlDir() throws Exception {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve("01_platform/02_sql/ddl/schema_manifest.json");
            if (Files.isRegularFile(candidate)) {
                return candidate.getParent();
            }
            current = current.getParent();
        }
        throw new IllegalStateException("cannot locate 01_platform/02_sql/ddl");
    }
}
