package com.trading.compute.signaljob;

import com.trading.common.schema.CandleTableSchema;
import java.util.List;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;

/**
 * Read-only candle-table contract checks (CANDLE-KV-REPLAY-001 P4/A4.x;
 * tracker 14 P1 — CANDLE-SCHEMA-002).
 *
 * <p>Before the graph is built (and before any Fluss sink/source serialization
 * is wired), the job must prove that the deployed tables match the contract
 * the dual-write relies on:
 *
 * <ul>
 *   <li>{@code feature_candles_15s} — immutable LOG: <b>no</b> primary key
 *       (append-only; a PK would turn it into a KV table and silently change
 *       replay semantics), bucket.key exactly {@code instrument_token} and 16
 *       buckets (the per-ticker colocation anchor the KV twin mirrors).
 *   <li>{@code feature_candles_15s_current} — KV: primary key exactly
 *       {@code (instrument_token, window_start)}, bucket.key
 *       {@code instrument_token} (a strict subset of the PK, as Fluss requires
 *       {@code pk ⊇ bucketKey}), 16 buckets.
 * </ul>
 *
 * <p>Both tables must also carry the exact 15-column v2 schema — names in DDL
 * order, Fluss type root per column, and (where Fluss enforces it) column
 * nullability. {@code TableInfo.getSchema().getColumns()} is compared against
 * the shared {@link CandleTableSchema#COLUMNS} /
 * {@link CandleTableSchema#COLUMN_TYPE_ROOTS} contract column by column so a
 * future DDL that renames, reorders, widens, or re-types a column fails
 * startup instead of silently mis-serializing rows.
 *
 * <p>Nullability enforcement follows what Fluss actually guarantees in live
 * metadata (verified against the dev cluster 2026-08-10): a LOG table reports
 * every column nullable (DDL NOT NULL is not carried into LOG metadata), and
 * a KV table reports only its PK columns non-nullable. The validator enforces
 * non-nullability on the KV PK columns only; DDL-level NOT NULL on non-PK
 * columns is reported by {@link #schemaReport(TableInfo)} as divergence
 * information, not a failure — the storage layer cannot enforce it.
 *
 * <p>Violations fail startup with a {@link ContractViolation} — never a
 * degraded write. The checks are pure {@link TableInfo} inspection so they are
 * unit-testable without a cluster ({@code TableInfo.of(...)} with a built
 * {@code TableDescriptor}); {@link SignalJob#preflightTableContracts} feeds
 * them from the live cluster before the environment is created.
 */
public final class CandleTableContractValidator {

    /** Thrown when a deployed table violates the dual-write contract. */
    public static final class ContractViolation extends IllegalStateException {
        public ContractViolation(String message) {
            super(message);
        }
    }

    private CandleTableContractValidator() {}

    /** LOG twin: append-only, no primary key, instrument_token bucketing, exact schema. */
    public static void validateLogTable(TableInfo info) {
        if (info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "LOG table " + info.getTablePath() + " must have NO primary key (append-only), "
                            + "but is a KV table with PK " + info.getPrimaryKeys()
                            + " (CANDLE-KV-REPLAY-001 P4)");
        }
        validateSchema(info);
        validateRouting(info);
    }

    /** KV current-state table: PK exactly (instrument_token, window_start), same routing. */
    public static void validateCanonicalKvTable(TableInfo info) {
        if (!info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must be a KV table with PRIMARY KEY "
                            + CandleTableSchema.KEY_COLUMNS + ", but has no primary key"
                            + " (CANDLE-KV-REPLAY-001 P4)");
        }
        List<String> pk = info.getPrimaryKeys();
        if (!CandleTableSchema.KEY_COLUMNS.equals(pk)) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must have PRIMARY KEY exactly "
                            + CandleTableSchema.KEY_COLUMNS + " (column order matters), got " + pk
                            + " (CANDLE-KV-REPLAY-001 P4)");
        }
        validateSchema(info);
        validateRouting(info);
    }

    /**
     * Exact 15-column schema check (tracker 14 P1, CANDLE-SCHEMA-002): column
     * count, then per-column name (DDL order), Fluss type root, and — for KV
     * tables — PK-column nullability, against the shared contract.
     */
    private static void validateSchema(TableInfo info) {
        Schema schema = info.getSchema();
        List<Schema.Column> columns = schema.getColumns();
        if (columns.size() != CandleTableSchema.FIELD_COUNT) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must carry exactly "
                            + CandleTableSchema.FIELD_COUNT + " columns (v2 candle contract), got "
                            + columns.size() + " (tracker 14 P1, CANDLE-SCHEMA-002)");
        }
        List<String> expectedNames = CandleTableSchema.COLUMNS;
        List<String> expectedTypes = CandleTableSchema.COLUMN_TYPE_ROOTS;
        for (int i = 0; i < columns.size(); i++) {
            Schema.Column column = columns.get(i);
            String actualType = column.getDataType().getTypeRoot().name();
            if (!expectedNames.get(i).equals(column.getName())) {
                throw new ContractViolation(
                        "table " + info.getTablePath() + " column #" + i + " must be named '"
                                + expectedNames.get(i) + "' (DDL order), got '" + column.getName()
                                + "' (tracker 14 P1, CANDLE-SCHEMA-002)");
            }
            if (!expectedTypes.get(i).equals(actualType)) {
                throw new ContractViolation(
                        "table " + info.getTablePath() + " column '" + column.getName()
                                + "' must be " + expectedTypes.get(i) + " (v2 candle contract), got "
                                + actualType + " (tracker 14 P1, CANDLE-SCHEMA-002)");
            }
        }
        // Nullability is enforced only where Fluss enforces it: KV primary-key
        // columns are non-nullable in live metadata; everything else on a LOG
        // or KV table is reported by schemaReport(), not failed.
        if (info.hasPrimaryKey()) {
            for (String pkColumn : info.getPrimaryKeys()) {
                Schema.Column column = schema.getColumns().stream()
                        .filter(c -> c.getName().equals(pkColumn))
                        .findFirst()
                        .orElseThrow(() -> new ContractViolation(
                                "table " + info.getTablePath() + " primary key column '" + pkColumn
                                        + "' missing from schema (tracker 14 P1)"));
                if (column.getDataType().isNullable()) {
                    throw new ContractViolation(
                            "table " + info.getTablePath() + " primary key column '" + pkColumn
                                    + "' must be NOT NULL (Fluss enforces non-nullable KV PKs), "
                                    + "got nullable (tracker 14 P1, CANDLE-SCHEMA-002)");
                }
            }
        }
    }

    /**
     * Human-readable schema report for the startup log (and evidence):
     * column name, live type root, live nullability, DDL intent, and a
     * divergence marker where the DDL's NOT NULL is not carried into live
     * metadata. Never throws — the strict checks are {@code validate*Table}.
     */
    public static String schemaReport(TableInfo info) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("table ").append(info.getTablePath()).append(" schema=[");
        List<Schema.Column> columns = info.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Schema.Column c = columns.get(i);
            boolean ddlNotNull = !CandleTableSchema.COLUMN_NULLABLE_IN_DDL.get(i);
            boolean liveNotNull = !c.getDataType().isNullable();
            sb.append(c.getName()).append(':').append(c.getDataType().getTypeRoot().name());
            if (liveNotNull) {
                sb.append(" NOT NULL");
            } else {
                sb.append(ddlNotNull ? " nullable(DDL NOT NULL not carried)" : " nullable");
            }
        }
        return sb.append(']').toString();
    }

    /** Bucket key exactly instrument_token, 16 buckets (colocation with the LOG twin). */
    private static void validateRouting(TableInfo info) {
        if (!info.hasBucketKey()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must set bucket.key explicitly, but uses the default"
                            + " (CANDLE-KV-REPLAY-001 P4)");
        }
        List<String> bucketKeys = info.getBucketKeys();
        if (!List.of(CandleTableSchema.BUCKET_KEY).equals(bucketKeys)) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must be distributed by bucket.key exactly ["
                            + CandleTableSchema.BUCKET_KEY + "] (per-ticker colocation with the LOG twin), got "
                            + bucketKeys + " (CANDLE-KV-REPLAY-001 P4)");
        }
        int buckets = info.getNumBuckets();
        if (buckets != CandleTableSchema.BUCKET_COUNT) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must have " + CandleTableSchema.BUCKET_COUNT
                            + " buckets (mirrors the LOG twin), got " + buckets
                            + " (CANDLE-KV-REPLAY-001 P4)");
        }
    }
}
