package com.trading.ingestion;

import java.util.List;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time DDL bootstrap. Creates the {@code default} database and all
 * 19 platform tables with correct bucket routing. Fluss infers the
 * column schema from the first {@code AppendWriter.append(GenericRow)}
 * call, so we only need table existence + bucket distribution.
 *
 * <p>Idempotent — skips already-existing resources.
 * Call once at ingestion startup; blocks until all tables confirmed.
 */
public final class DdlBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(DdlBootstrap.class);

    private DdlBootstrap() {}

    /**
     * Read-only verification that the default database and all expected tables
     * exist with the expected column count. Never creates, drops, or alters
     * anything. This is the default production start path.
     *
     * @return true if every expected table exists with the expected column count
     */
    public static boolean verifyTables(String bootstrapServers) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        LOG.info("ddl-bootstrap: verifying schema at {} (read-only) ...", bootstrapServers);

        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {

            if (!databaseExists(admin, "default")) {
                LOG.error("ddl-bootstrap: database 'default' does not exist — run DDL first");
                return false;
            }

            int ok = 0, missing = 0, schemaMismatch = 0;
            for (var entry : ALL_TABLES.entrySet()) {
                String name = entry.getKey();
                TableDescriptor td = entry.getValue();
                TablePath path = TablePath.of("default", name);

                try {
                    org.apache.fluss.metadata.TableInfo ti = c.getTable(path).getTableInfo();
                    int existingCols = ti.getRowType().getFieldCount();
                    int expectedCols = td.getSchema().getColumns().size();
                    if (existingCols != expectedCols) {
                        LOG.error("ddl-bootstrap: default.{} has {} cols, expected {} — schema mismatch",
                                name, existingCols, expectedCols);
                        schemaMismatch++;
                    } else {
                        ok++;
                    }
                } catch (Exception e) {
                    LOG.error("ddl-bootstrap: default.{} missing or not readable: {}", name, e.getMessage());
                    missing++;
                }
            }

            LOG.info("ddl-bootstrap: verified {} tables ok, {} missing, {} schema-mismatch",
                    ok, missing, schemaMismatch);
            return missing == 0 && schemaMismatch == 0;

        } catch (Exception e) {
            LOG.error("ddl-bootstrap: connection failed — {}", e.getMessage());
            return false;
        }
    }

    private static boolean databaseExists(Admin admin, String name) {
        try {
            return admin.listDatabases().get().contains(name);
        } catch (Exception e) {
            LOG.warn("ddl-bootstrap: could not list databases: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Ensure all platform tables exist on the given Fluss cluster.
     * MUTATES: creates or drops/recreates tables. Only for local development.
     * Returns true if successful.
     */
    public static boolean ensureTables(String bootstrapServers) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        LOG.info("ddl-bootstrap: connecting to {} ...", bootstrapServers);

        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {

            ensureDatabase(admin, "default");

            int ok = 0, failed = 0;
            for (var entry : ALL_TABLES.entrySet()) {
                String name = entry.getKey();
                TableDescriptor td = entry.getValue();
                TablePath path = TablePath.of("default", name);

                try {
                    admin.createTable(path, td, false).get();
                    LOG.info("ddl-bootstrap: ✓ default.{}", name);
                    ok++;
                } catch (Exception e) {
                    String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                    if (msg.contains("already exist")) {
                        // Table exists. Check column count — drop + recreate if schema mismatch.
                        try {
                            org.apache.fluss.metadata.TableInfo ti = c.getTable(path).getTableInfo();
                            int existingCols = ti.getRowType().getFieldCount();
                            int expectedCols = td.getSchema().getColumns().size();
                            if (existingCols != expectedCols) {
                                LOG.warn("ddl-bootstrap: default.{} has {} cols, expected {} — dropping and recreating",
                                        name, existingCols, expectedCols);
                                admin.dropTable(path, false).get();
                                admin.createTable(path, td, false).get();
                                LOG.info("ddl-bootstrap: ✓ default.{} recreated with {} cols", name, expectedCols);
                            } else {
                                LOG.debug("ddl-bootstrap: default.{} already exists ({} cols)", name, existingCols);
                            }
                            ok++;
                        } catch (Exception inner) {
                            LOG.warn("ddl-bootstrap: default.{} already exists, skip schema check: {}", name, inner.getMessage());
                            ok++;
                        }
                    } else {
                        LOG.error("ddl-bootstrap: ✗ default.{} — {}", name, e.getMessage());
                        failed++;
                    }
                }
            }

            LOG.info("ddl-bootstrap: {} tables ok, {} failed", ok, failed);
            return failed == 0;

        } catch (Exception e) {
            LOG.error("ddl-bootstrap: connection failed — {}", e.getMessage());
            return false;
        }
    }

    private static void ensureDatabase(Admin admin, String name) throws Exception {
        try {
            admin.createDatabase(name, DatabaseDescriptor.builder().build(), false).get();
            LOG.info("ddl-bootstrap: database '{}' created", name);
        } catch (Exception e) {
            if (e.getMessage() != null
                    && e.getMessage().toLowerCase().contains("already exist")) {
                LOG.info("ddl-bootstrap: database '{}' exists", name);
            } else {
                throw e;
            }
        }
    }

    // ── Table registry: name → descriptor (bucket routing only) ──────

    /** Full 28-column schema for raw_table_1 matching the DDL. */
    private static final Schema RAW_TABLE_1_SCHEMA = Schema.newBuilder()
            .column("event_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("fingerprint_version", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_id", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("instrument_type", org.apache.fluss.types.DataTypes.STRING())
            .column("strike_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("expiry", org.apache.fluss.types.DataTypes.BIGINT())
            .column("option_type", org.apache.fluss.types.DataTypes.STRING())
            .column("event_time", org.apache.fluss.types.DataTypes.BIGINT())
            .column("ingest_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("ack_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("tick_type", org.apache.fluss.types.DataTypes.STRING())
            .column("last_price_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("last_qty", org.apache.fluss.types.DataTypes.BIGINT())
            .column("bid_price_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("bid_qty", org.apache.fluss.types.DataTypes.BIGINT())
            .column("ask_price_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("ask_qty", org.apache.fluss.types.DataTypes.BIGINT())
            .column("raw_payload", org.apache.fluss.types.DataTypes.BYTES())
            .column("payload_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("decoder_version", org.apache.fluss.types.DataTypes.STRING())
            .column("protocol_version", org.apache.fluss.types.DataTypes.STRING())
            .column("validity_state", org.apache.fluss.types.DataTypes.STRING())
            .column("validity_reason", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /** Full 18-column schema for Postback_Quarantine matching QuarantineWriter. */
    private static final Schema POSTBACK_QUARANTINE_SCHEMA = Schema.newBuilder()
            .column("quarantine_id", org.apache.fluss.types.DataTypes.STRING())
            .column("postback_event_id", org.apache.fluss.types.DataTypes.STRING())
            .column("reason", org.apache.fluss.types.DataTypes.STRING())
            .column("broker_order_id", org.apache.fluss.types.DataTypes.STRING())
            .column("client_order_ref", org.apache.fluss.types.DataTypes.STRING())
            .column("broker_status", org.apache.fluss.types.DataTypes.STRING())
            .column("broker_timestamp", org.apache.fluss.types.DataTypes.BIGINT())
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("raw_payload", org.apache.fluss.types.DataTypes.BYTES())
            .column("payload_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("detected_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("status", org.apache.fluss.types.DataTypes.STRING())
            .column("resolution_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("resolution_note", org.apache.fluss.types.DataTypes.STRING())
            .column("operator_identity", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /** Full 15-column schema for suspected_discontinuities matching DiscontinuityWriter. */
    private static final Schema DISCONTINUITY_SCHEMA = Schema.newBuilder()
            .column("discontinuity_id", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_id", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("reason", org.apache.fluss.types.DataTypes.STRING())
            .column("before_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("after_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("before_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("after_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("detected_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("status", org.apache.fluss.types.DataTypes.STRING())
            .column("note", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /** Full 21-column schema for Safety_Halt_Requests matching the migrated v2 DDL. */
    private static final Schema SAFETY_HALT_SCHEMA = Schema.newBuilder()
            .column("halt_request_id", org.apache.fluss.types.DataTypes.STRING())
            .column("account_scope_id", org.apache.fluss.types.DataTypes.STRING())
            .column("portfolio_id", org.apache.fluss.types.DataTypes.STRING())
            .column("execution_partition_id", org.apache.fluss.types.DataTypes.STRING())
            .column("source_component", org.apache.fluss.types.DataTypes.STRING())
            .column("source_instance", org.apache.fluss.types.DataTypes.STRING())
            .column("reason_code", org.apache.fluss.types.DataTypes.STRING())
            .column("reason_detail", org.apache.fluss.types.DataTypes.STRING())
            .column("detection_time", org.apache.fluss.types.DataTypes.BIGINT())
            .column("source_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("evidence_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("application_result", org.apache.fluss.types.DataTypes.STRING())
            .column("applied_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .column("slot_id", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("manifest_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("assigned_token_set_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("state", org.apache.fluss.types.DataTypes.STRING())
            .column("evidence_reference", org.apache.fluss.types.DataTypes.STRING())
            .column("contract_version", org.apache.fluss.types.DataTypes.INT())
            .build();

    // Minimal schema for other platform tables (not yet accessed by services).
    // Each table's schema will be expanded when the owning service is built.
    private static final Schema MINIMAL_SCHEMA = Schema.newBuilder()
            .column("instrument_token", org.apache.fluss.types.DataTypes.STRING())
            .column("execution_partition_id", org.apache.fluss.types.DataTypes.STRING())
            .column("portfolio_id", org.apache.fluss.types.DataTypes.STRING())
            .column("account_scope_id", org.apache.fluss.types.DataTypes.STRING())
            .column("broker_order_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instruction_id", org.apache.fluss.types.DataTypes.STRING())
            .column("client_order_ref", org.apache.fluss.types.DataTypes.STRING())
            .build();

    private static TableDescriptor logTable(String... bucketKeys) {
        return TableDescriptor.builder()
                .schema(MINIMAL_SCHEMA)
                .distributedBy(4, bucketKeys)
                .build();
    }

    private static TableDescriptor kvTable(String... bucketKeys) {
        return TableDescriptor.builder()
                .schema(MINIMAL_SCHEMA)
                .distributedBy(4, bucketKeys)
                .build();
    }

    private static final java.util.Map<String, TableDescriptor> ALL_TABLES =
            java.util.Map.ofEntries(
                    java.util.Map.entry("raw_table_1",
                            TableDescriptor.builder()
                                    .schema(RAW_TABLE_1_SCHEMA)
                                    .distributedBy(16, "instrument_token")
                                    .build()),
                    java.util.Map.entry("feature_candles_15s",
                            logTable("instrument_token")),
                    java.util.Map.entry("Signal_Candidates",
                            logTable("instrument_token")),
                    java.util.Map.entry("Ranking_Results",
                            logTable("execution_partition_id")),
                    java.util.Map.entry("Trade_Decisions",
                            logTable("execution_partition_id")),
                    java.util.Map.entry("Fills",
                            logTable("portfolio_id")),
                    java.util.Map.entry("Execution_Audit",
                            logTable("execution_partition_id")),
                    java.util.Map.entry("Postback_Quarantine",
                            TableDescriptor.builder().schema(POSTBACK_QUARANTINE_SCHEMA).distributedBy(8, "quarantine_id").build()),
                    java.util.Map.entry("Safety_Halt_Requests",
                            TableDescriptor.builder().schema(SAFETY_HALT_SCHEMA).distributedBy(4, "halt_request_id").build()),
                    java.util.Map.entry("suspected_discontinuities",
                            TableDescriptor.builder().schema(DISCONTINUITY_SCHEMA).distributedBy(4, "discontinuity_id").build()),
                    java.util.Map.entry("forming_bar",
                            kvTable("instrument_token")),
                    java.util.Map.entry("Order_Lifecycle",
                            kvTable("broker_order_id")),
                    java.util.Map.entry("Positions",
                            kvTable("portfolio_id")),
                    java.util.Map.entry("Execution_Gate",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "execution_partition_id").build()),
                    java.util.Map.entry("Execution_Attempts",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "instruction_id").build()),
                    java.util.Map.entry("Order_Correlation",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "client_order_ref").build()),
                    java.util.Map.entry("Portfolio_Reservations",
                            kvTable("portfolio_id")),
                    java.util.Map.entry("Postback_Projection_Ledger",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "broker_order_id").build()),
                    java.util.Map.entry("instruments",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "instrument_token").build())
            );
}
