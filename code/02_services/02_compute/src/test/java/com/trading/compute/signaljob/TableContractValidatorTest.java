package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.CandleTableSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Table-contract validator (tracker 14 P1 — CANDLE-SCHEMA-002 for the candle
 * LOG; tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001 for the signal LOG/KV
 * pair): each deployed table must carry the exact contract the write path
 * relies on — append-only LOG (no PK), KV current-state PK exactly
 * {@code [instrument_token]}, per-ticker routing (bucket.key
 * {@code instrument_token}, 16 buckets), and the exact frozen column schema
 * (names in DDL order, Fluss type root per column) — or startup fails
 * closed.
 */
@DisplayName("TableContractValidator")
class TableContractValidatorTest {

    private static final String LOG = "feature_candles_15s";
    private static final String SIGNAL_LOG = "Signal_Candidates";
    private static final String SIGNAL_CURRENT = "Signal_Candidates_current";
    private static final String TOKEN = "instrument_token";

    private static final List<String> CANDLE_NAMES = CandleTableSchema.COLUMNS;
    private static final List<String> CANDLE_TYPES = CandleTableSchema.COLUMN_TYPE_ROOTS;
    private static final List<String> SIGNAL_NAMES = Arrays.asList(SignalCandidatesTableColumns.NAMES);
    private static final List<String> SIGNAL_TYPES = SignalCandidatesTableColumns.TYPE_ROOTS;

    // ── candle LOG (CANDLE-SCHEMA-002) ──

    @Test
    @DisplayName("candle LOG without primary key and with instrument_token routing passes")
    void candleLogTableWithoutPkPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateCandleLogTable(
                candle(LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle LOG that gained a primary key is rejected (append-only contract)")
    void candleLogTableWithPkIsRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(
                        candle(LOG, List.of(TOKEN, "window_start"), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle LOG with exact 15-column v2 schema and all-nullable live metadata passes")
    void candleLogExactSchemaPasses() {
        // Live LOG metadata reports every column nullable (DDL NOT NULL is not
        // carried into LOG metadata — verified 2026-08-10); that must pass.
        assertDoesNotThrow(() -> TableContractValidator.validateCandleLogTable(
                candle(LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle wrong column count is rejected (14 or 16 columns)")
    void candleWrongColumnCountRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(CANDLE_TYPES);
        shortTypes.remove(14); // drop schema_version -> 14 columns
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(
                        candle(LOG, null, List.of(TOKEN), 16, shortTypes, false)));
        List<String> longTypes = new java.util.ArrayList<>(CANDLE_TYPES);
        longTypes.add("BIGINT"); // extra column -> 16
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(
                        candle(LOG, null, List.of(TOKEN), 16, longTypes, false)));
    }

    @Test
    @DisplayName("candle renamed column is rejected (name drift breaks writer layout)")
    void candleRenamedColumnRejected() {
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < CANDLE_NAMES.size(); i++) {
            String col = i == 1 ? "exchng" : CANDLE_NAMES.get(i);
            sb.column(col, dataType(CANDLE_TYPES.get(i), false));
        }
        TableInfo info = info(LOG, sb, List.of(TOKEN), 16);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(info));
    }

    @Test
    @DisplayName("candle reordered column is rejected (DDL index order is part of the contract)")
    void candleReorderedColumnRejected() {
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < CANDLE_NAMES.size(); i++) {
            String col = i == 1 ? CANDLE_NAMES.get(2)
                    : i == 2 ? CANDLE_NAMES.get(1)
                    : CANDLE_NAMES.get(i);
            sb.column(col, dataType(CANDLE_TYPES.get(i), false));
        }
        TableInfo info = info(LOG, sb, List.of(TOKEN), 16);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(info));
    }

    @Test
    @DisplayName("candle wrong type root per column is rejected (exchange STRING vs BIGINT)")
    void candleWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(CANDLE_TYPES);
        types.set(1, "BIGINT"); // exchange must be STRING
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(
                        candle(LOG, null, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("candle tick_count must be INTEGER, not BIGINT")
    void candleTickCountMustBeInteger() {
        List<String> types = new java.util.ArrayList<>(CANDLE_TYPES);
        types.set(10, "BIGINT");
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLogTable(
                        candle(LOG, null, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("candle schemaReport never throws and names the DDL-vs-live nullability divergence")
    void candleSchemaReportIsInformational() {
        TableInfo log = candle(LOG, null, List.of(TOKEN), 16);
        String report = TableContractValidator.schemaReport(log, CandleTableSchema.COLUMN_NULLABLE_IN_DDL);
        assertNotNull(report);
        assertTrue(report.contains("instrument_token:BIGINT"),
                "report must name live type roots, got: " + report);
        assertTrue(report.contains("nullable(DDL NOT NULL not carried)"),
                "report must surface the DDL-vs-live nullability divergence, got: " + report);
    }

    // ── signal LOG (SIGNAL-SCHEMA-001) ──

    @Test
    @DisplayName("signal LOG without primary key and with instrument_token routing passes")
    void signalLogTableWithoutPkPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateSignalLogTable(
                signal(SIGNAL_LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal LOG that gained a primary key is rejected (append-only contract)")
    void signalLogTableWithPkIsRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, List.of(TOKEN), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal LOG with exact 22-column v3 schema passes")
    void signalLogExactSchemaPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateSignalLogTable(
                signal(SIGNAL_LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal wrong column count is rejected (21 or 23 columns)")
    void signalWrongColumnCountRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(SIGNAL_TYPES);
        shortTypes.remove(21); // drop schema_version -> 21 columns
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 16, shortTypes, false)));
        List<String> longTypes = new java.util.ArrayList<>(SIGNAL_TYPES);
        longTypes.add("STRING"); // extra column -> 23
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 16, longTypes, false)));
    }

    @Test
    @DisplayName("signal wrong type root per column is rejected (instrument_token BIGINT vs STRING)")
    void signalWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(SIGNAL_TYPES);
        types.set(3, "STRING"); // instrument_token must be BIGINT
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("signal LOG with a non-instrument_token bucket key is rejected")
    void signalLogWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of("candidate_id"), 16)));
    }

    @Test
    @DisplayName("signal LOG with a bucket count other than 16 is rejected")
    void signalLogWrongBucketCountRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 17)));
    }

    // ── signal current-state KV (SIGNAL-SCHEMA-001) ──

    @Test
    @DisplayName("signal KV with PK exactly [instrument_token] and matching routing passes")
    void signalCurrentKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateSignalCurrentKvTable(
                signal(SIGNAL_CURRENT, List.of(TOKEN), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal KV without a primary key is rejected (current-state contract)")
    void signalCurrentKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal KV with a wider primary key is rejected (exact per-ticker current state)")
    void signalCurrentKvWrongPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, List.of(TOKEN, "detection_ts"),
                                List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal KV with a bucket key outside the primary key is rejected")
    void signalCurrentKvWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, List.of(TOKEN), List.of("candidate_id"), 16)));
    }

    @Test
    @DisplayName("signal KV with schema drift is rejected (21 columns)")
    void signalCurrentKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(SIGNAL_TYPES);
        shortTypes.remove(21);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, List.of(TOKEN), List.of(TOKEN), 16,
                                shortTypes, false)));
    }

    @Test
    @DisplayName("signal KV schemaReport names live types and DDL nullability intent")
    void signalCurrentKvSchemaReportIsInformational() {
        TableInfo kv = signal(SIGNAL_CURRENT, List.of(TOKEN), List.of(TOKEN), 16);
        String report = TableContractValidator.schemaReport(
                kv, SignalCandidatesTableColumns.COLUMN_NULLABLE_IN_DDL);
        assertNotNull(report);
        assertTrue(report.contains("instrument_token:BIGINT"),
                "report must name live type roots, got: " + report);
        assertTrue(report.contains("nullable(DDL NOT NULL not carried)"),
                "report must surface the DDL-vs-live nullability divergence, got: " + report);
    }

    // ── fixtures ──

    /**
     * Builds a TableInfo via the explicit 12-arg constructor (no cluster
     * needed). Verified against the decompiled ctor: slot 6 = bucketKeys,
     * slot 7 = partitionKeys, and the primaryKeys field is derived from the
     * Schema's {@code primaryKey(...)} — so {@code schemaPk} drives both the
     * PK assertions and the routing defaults.
     *
     * <p>The schema mirrors the live dev-cluster metadata (verified
     * 2026-08-10): all columns present in DDL order with the exact type
     * roots, all nullable (LOG metadata never carries NOT NULL).
     * {@code columnTypes} / {@code pkNonNullable} override that default to
     * build the negative fixtures.
     */
    private static TableInfo candle(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_NAMES, CANDLE_TYPES, null, true);
    }

    private static TableInfo candle(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_NAMES, CANDLE_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo signal(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, SIGNAL_NAMES, SIGNAL_TYPES, null, true);
    }

    private static TableInfo signal(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, SIGNAL_NAMES, SIGNAL_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo table(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> names, List<String> typeRoots, List<String> columnTypes,
            boolean pkNonNullable) {
        Schema.Builder sb = Schema.newBuilder();
        int cols = columnTypes == null ? names.size() : columnTypes.size();
        for (int i = 0; i < cols; i++) {
            String col = i < names.size() ? names.get(i) : "extra_col_" + i;
            String type = columnTypes == null ? typeRoots.get(i) : columnTypes.get(i);
            sb.column(col, dataType(type, pkNonNullable && schemaPk != null && schemaPk.contains(col)));
        }
        if (schemaPk != null) {
            sb.primaryKey(schemaPk);
        }
        return info(name, sb, bucketKeys, numBuckets);
    }

    private static TableInfo info(String name, Schema.Builder sb, List<String> bucketKeys,
            int numBuckets) {
        return new TableInfo(TablePath.of("default", name), 1L, 1, sb.build(), bucketKeys,
                List.of(), numBuckets, new Configuration(), new Configuration(), null, 0L, 0L);
    }

    private static org.apache.fluss.types.DataType dataType(String typeRoot, boolean notNull) {
        org.apache.fluss.types.DataType t;
        switch (typeRoot) {
            case "BIGINT":
                t = DataTypes.BIGINT();
                break;
            case "STRING":
                t = DataTypes.STRING();
                break;
            case "INTEGER":
                t = DataTypes.INT();
                break;
            default:
                throw new IllegalArgumentException("unexpected type root " + typeRoot);
        }
        return notNull ? t.copy(false) : t.copy(true);
    }
}
