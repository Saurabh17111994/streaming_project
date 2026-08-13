package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared scratch-table plumbing for the env-gated SignalJob integration
 * tests: canonical DDL 03/05/23 schema mirrors plus create/track/drop.
 *
 * <p>Two consumers — {@link CandleGraphReplayIntegrationTest} and
 * {@link SignalJobOperatorUidTest} — so each DDL mirror exists in exactly one
 * place. Created names are tracked in a static registry; {@link
 * #dropCreated(Admin, Duration)} tears them down best-effort (unique suffixes
 * make leftovers harmless).
 */
final class ScratchTables {

    private static final Logger LOG = LoggerFactory.getLogger(ScratchTables.class);

    private static final List<String> CREATED = new ArrayList<>();

    private ScratchTables() {}

    /** 15-column candle LOG schema (mirrors feature_candles_15s, DDL 03). */
    static Schema candleSchema() {
        return Schema.newBuilder()
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("window_start", DataTypes.BIGINT())
                .column("window_end", DataTypes.BIGINT())
                .column("open_paise", DataTypes.BIGINT())
                .column("high_paise", DataTypes.BIGINT())
                .column("low_paise", DataTypes.BIGINT())
                .column("close_paise", DataTypes.BIGINT())
                .column("volume", DataTypes.BIGINT())
                .column("tick_count", DataTypes.INT())
                .column("algorithm_version", DataTypes.STRING())
                .column("configuration_version", DataTypes.STRING())
                .column("output_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .build();
    }

    /** 22-column signal LOG schema mirroring DDL 05 v3 (no PK — append-only audit). */
    static Schema signalLogSchema() {
        return signalColumns(Schema.newBuilder()).build();
    }

    /** 22-column signal current-state KV schema mirroring DDL 23 (PK instrument_token). */
    static Schema signalCurrentSchema() {
        return signalColumns(Schema.newBuilder()).primaryKey("instrument_token").build();
    }

    static Table create(Connection connection, Admin admin, String name, Schema schema,
            List<String> pk, int bucketCount, String what, Duration timeout) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                // LOG tables: instrument_token routing (mirrors raw_table_1 /
                // feature_candles_15s). KV tables: bucket key must be a subset
                // of the PK — the current-state KV uses instrument_token (DDL 23).
                .distributedBy(bucketCount, pk == null ? "instrument_token" : pk.get(0))
                .build();
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, td, false).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        CREATED.add(name);
        LOG.info("p6: created scratch {} table {}", what, name);
        return connection.getTable(path);
    }

    /** Best-effort drop of every table created through this utility. */
    static void dropCreated(Admin admin, Duration timeout) throws Exception {
        for (String table : List.copyOf(CREATED)) {
            try {
                admin.dropTable(TablePath.of("default", table), false)
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("p6: dropped scratch table {}", table);
            } catch (Exception e) {
                LOG.warn("p6: drop {} failed: {}", table, e.getMessage());
            }
            CREATED.remove(table);
        }
    }

    private static Schema.Builder signalColumns(Schema.Builder b) {
        return b.column("candidate_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("strategy_id", DataTypes.STRING())
                .column("strategy_version", DataTypes.STRING())
                .column("rule_id", DataTypes.STRING())
                .column("detection_ts", DataTypes.BIGINT())
                .column("evaluation_ts", DataTypes.BIGINT())
                .column("action", DataTypes.STRING())
                .column("side", DataTypes.STRING())
                .column("quantity", DataTypes.BIGINT())
                .column("order_type", DataTypes.STRING())
                .column("limit_price_paise", DataTypes.BIGINT())
                .column("score_inputs", DataTypes.STRING())
                .column("formation_snapshot_ref", DataTypes.STRING())
                .column("validity_reason", DataTypes.STRING())
                .column("supersedes_candidate_id", DataTypes.STRING())
                .column("superseded_by_candidate_id", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING());
    }
}
