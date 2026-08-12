package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.CandleTableSchema;
import java.util.List;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Candle-table contract validator (CANDLE-KV-REPLAY-001 P4/A4.x; tracker 14
 * P1 — CANDLE-SCHEMA-002): the LOG twin must be append-only and the KV twin
 * must carry exactly the canonical PK with the same routing, and both must
 * carry the exact 15-column v2 schema (names in DDL order, Fluss type root
 * per column, KV PK columns non-nullable) — or startup fails closed.
 */
@DisplayName("CandleTableContractValidator")
class CandleTableContractValidatorTest {

    private static final String LOG = "feature_candles_15s";
    private static final String KV = "feature_candles_15s_current";

    @Test
    @DisplayName("LOG table without primary key and with instrument_token routing passes")
    void logTableWithoutPkPasses() {
        assertDoesNotThrow(() -> CandleTableContractValidator.validateLogTable(
                table(LOG, null, List.of(), List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("LOG table that somehow gained a primary key is rejected (append-only contract)")
    void logTableWithPkIsRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(
                        table(LOG, List.of("instrument_token", "window_start"),
                                List.of("instrument_token", "window_start"),
                                List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("canonical KV table passes: exact PK order and routing")
    void canonicalKvPasses() {
        assertDoesNotThrow(() -> CandleTableContractValidator.validateCanonicalKvTable(
                table(KV, List.of("instrument_token", "window_start"),
                        List.of("instrument_token", "window_start"),
                        List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("KV table without a primary key is rejected")
    void kvWithoutPkIsRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, null, List.of(), List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("KV table with wrong PK order is rejected (column order matters)")
    void kvWrongPkOrderIsRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, List.of("window_start", "instrument_token"),
                                List.of("window_start", "instrument_token"),
                                List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("KV table with a different PK column set is rejected")
    void kvWrongPkColumnsAreRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, List.of("instrument_token"),
                                List.of("instrument_token"),
                                List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("KV table with bucket key different from instrument_token is rejected")
    void kvWrongBucketKeyIsRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, List.of("instrument_token", "window_start"),
                                List.of("instrument_token", "window_start"),
                                List.of("window_start"), 16)));
    }

    @Test
    @DisplayName("KV table with default (unset) bucket key is rejected — colocation would break")
    void kvDefaultBucketKeyIsRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, List.of("instrument_token", "window_start"),
                                List.of("instrument_token", "window_start"),
                                List.of(), 16)));
    }

    @Test
    @DisplayName("KV table with wrong bucket count is rejected (must mirror the LOG twin)")
    void kvWrongBucketCountIsRejected() {
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, List.of("instrument_token", "window_start"),
                                List.of("instrument_token", "window_start"),
                                List.of("instrument_token"), 8)));
    }

    @Test
    @DisplayName("the two contracts are mutually exclusive (cross-check)")
    void contractsAreMutuallyExclusive() {
        TableInfo log = table(LOG, null, List.of(), List.of("instrument_token"), 16);
        TableInfo kv = table(KV, List.of("instrument_token", "window_start"),
                List.of("instrument_token", "window_start"),
                List.of("instrument_token"), 16);
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(log));
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(kv));
    }

    /**
     * Builds a TableInfo via the explicit 12-arg constructor (no cluster
     * needed). Verified against the decompiled ctor: slot 6 = bucketKeys,
     * slot 7 = partitionKeys, and the primaryKeys field is derived from the
     * Schema's {@code primaryKey(...)} — so {@code schemaPk} drives both the
     * PK assertions and the routing defaults.
     *
     * <p>The schema mirrors the live dev-cluster metadata (verified
     * 2026-08-10): all columns present in DDL order with the exact v2 type
     * roots; PK columns non-nullable (Fluss enforces this on KV), non-PK
     * columns nullable. {@code columnTypes} / {@code pkNonNullable} override
     * that default to build the negative fixtures.
     */
    private static TableInfo table(String name, List<String> schemaPk, List<String> ignoredPk,
            List<String> bucketKeys, int numBuckets) {
        return table(name, schemaPk, ignoredPk, bucketKeys, numBuckets, null, true);
    }

    private static TableInfo table(String name, List<String> schemaPk, List<String> ignoredPk,
            List<String> bucketKeys, int numBuckets, List<String> columnTypes,
            boolean pkNonNullable) {
        Schema.Builder sb = Schema.newBuilder();
        int cols = columnTypes == null ? CandleTableSchema.COLUMNS.size() : columnTypes.size();
        for (int i = 0; i < cols; i++) {
            String col = i < CandleTableSchema.COLUMNS.size()
                    ? CandleTableSchema.COLUMNS.get(i)
                    : "extra_col_" + i;
            String type = columnTypes == null ? CandleTableSchema.COLUMN_TYPE_ROOTS.get(i)
                    : columnTypes.get(i);
            sb.column(col, dataType(type, pkNonNullable && schemaPk != null && schemaPk.contains(col)));
        }
        if (schemaPk != null) {
            sb.primaryKey(schemaPk);
        }
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

    // ── tracker 14 P1: exact 15-column schema checks (CANDLE-SCHEMA-002) ──

    @Test
    @DisplayName("LOG table with exact 15-column v2 schema and all-nullable live metadata passes")
    void logTableExactSchemaPasses() {
        // Live LOG metadata reports every column nullable (DDL NOT NULL is not
        // carried into LOG metadata — verified 2026-08-10); that must pass.
        assertDoesNotThrow(() -> CandleTableContractValidator.validateLogTable(
                table(LOG, null, List.of(), List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("KV table with exact 15-column schema and non-nullable PK passes")
    void kvTableExactSchemaPasses() {
        assertDoesNotThrow(() -> CandleTableContractValidator.validateCanonicalKvTable(
                table(KV, List.of("instrument_token", "window_start"),
                        List.of("instrument_token", "window_start"),
                        List.of("instrument_token"), 16)));
    }

    @Test
    @DisplayName("wrong column count is rejected (14 or 16 columns)")
    void wrongColumnCountRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(CandleTableSchema.COLUMN_TYPE_ROOTS);
        shortTypes.remove(14); // drop schema_version -> 14 columns
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(
                        table(LOG, null, List.of(), List.of("instrument_token"), 16,
                                shortTypes, false)));
        List<String> longTypes = new java.util.ArrayList<>(CandleTableSchema.COLUMN_TYPE_ROOTS);
        longTypes.add("BIGINT"); // extra column -> 16
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateCanonicalKvTable(
                        table(KV, List.of("instrument_token", "window_start"),
                                List.of("instrument_token", "window_start"),
                                List.of("instrument_token"), 16, longTypes, true)));
    }

    @Test
    @DisplayName("renamed column is rejected (name drift breaks writer layout)")
    void renamedColumnRejected() {
        List<String> types = new java.util.ArrayList<>(CandleTableSchema.COLUMN_TYPE_ROOTS);
        // Build with a wrong NAME at index 1 by swapping COLUMNS order via a
        // custom names list is not supported by the helper — instead use the
        // Schema.Builder directly for the name-level fixture.
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < CandleTableSchema.COLUMNS.size(); i++) {
            String col = i == 1 ? "exchng" : CandleTableSchema.COLUMNS.get(i);
            sb.column(col, dataType(CandleTableSchema.COLUMN_TYPE_ROOTS.get(i), false));
        }
        TableInfo info = new TableInfo(TablePath.of("default", LOG), 1L, 1, sb.build(),
                List.of("instrument_token"), List.of(), 16,
                new Configuration(), new Configuration(), null, 0L, 0L);
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(info));
    }

    @Test
    @DisplayName("reordered column is rejected (DDL index order is part of the contract)")
    void reorderedColumnRejected() {
        // Swap the names of indices 1 and 2 (exchange <-> symbol). Names stay
        // the same set but drift from DDL order — the writer layout derives
        // from CandleTableSchema.COLUMNS, so a reordered live schema silently
        // mis-serializes rows. Must fail closed.
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < CandleTableSchema.COLUMNS.size(); i++) {
            String col = i == 1 ? CandleTableSchema.COLUMNS.get(2)
                    : i == 2 ? CandleTableSchema.COLUMNS.get(1)
                    : CandleTableSchema.COLUMNS.get(i);
            sb.column(col, dataType(CandleTableSchema.COLUMN_TYPE_ROOTS.get(i), false));
        }
        TableInfo info = new TableInfo(TablePath.of("default", LOG), 1L, 1, sb.build(),
                List.of("instrument_token"), List.of(), 16,
                new Configuration(), new Configuration(), null, 0L, 0L);
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(info));
    }

    @Test
    @DisplayName("wrong type root per column is rejected (exchange STRING vs BIGINT)")
    void wrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(CandleTableSchema.COLUMN_TYPE_ROOTS);
        types.set(1, "BIGINT"); // exchange must be STRING
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(
                        table(LOG, null, List.of(), List.of("instrument_token"), 16,
                                types, false)));
    }

    @Test
    @DisplayName("tick_count must be INTEGER, not BIGINT")
    void tickCountMustBeInteger() {
        List<String> types = new java.util.ArrayList<>(CandleTableSchema.COLUMN_TYPE_ROOTS);
        types.set(10, "BIGINT");
        assertThrows(CandleTableContractValidator.ContractViolation.class,
                () -> CandleTableContractValidator.validateLogTable(
                        table(LOG, null, List.of(), List.of("instrument_token"), 16,
                                types, false)));
    }

    @Test
    @DisplayName("Fluss itself forces KV PK columns non-nullable at Schema build (validator's live-metadata assumption)")
    void flussEnforcesNonNullablePkAtSchemaBuild() {
        // A nullable-PK TableInfo cannot be constructed: Schema.Builder.primaryKey()
        // automatically marks PK columns NOT NULL (verified via probe 2026-08-10 —
        // this is the same mechanism that yields non-nullable PKs in live KV
        // metadata). The validator's KV PK nullability check (P1,
        // CANDLE-SCHEMA-002) is a defensive guard for that guarantee.
        TableInfo kv = table(KV, List.of("instrument_token", "window_start"),
                List.of("instrument_token", "window_start"),
                List.of("instrument_token"), 16, null, false);
        for (String pkCol : kv.getPrimaryKeys()) {
            for (Schema.Column c : kv.getSchema().getColumns()) {
                if (c.getName().equals(pkCol)) {
                    assertFalse(c.getDataType().isNullable(),
                            "Fluss must build PK column " + pkCol + " as NOT NULL");
                }
            }
        }
        assertDoesNotThrow(() -> CandleTableContractValidator.validateCanonicalKvTable(kv));
    }

    @Test
    @DisplayName("schemaReport never throws and names the DDL-vs-live nullability divergence")
    void schemaReportIsInformational() {
        TableInfo log = table(LOG, null, List.of(), List.of("instrument_token"), 16);
        String report = CandleTableContractValidator.schemaReport(log);
        assertNotNull(report);
        assertTrue(report.contains("instrument_token:BIGINT"),
                "report must name live type roots, got: " + report);
        assertTrue(report.contains("nullable(DDL NOT NULL not carried)"),
                "report must surface the DDL-vs-live nullability divergence, got: " + report);
    }
}
