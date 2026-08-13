package com.trading.compute.signaljob;

import com.trading.common.schema.CandleTableSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;

/**
 * Read-only table-contract checks run before the graph is built (and before
 * any Fluss sink/source serialization is wired): the job must prove that the
 * deployed tables match the contracts the write paths rely on.
 *
 * <ul>
 *   <li>{@code feature_candles_15s} — immutable candle LOG: <b>no</b> primary
 *       key (append-only; a PK would turn it into a KV table and silently
 *       change replay semantics), bucket.key exactly {@code instrument_token},
 *       16 buckets, exact 15-column v2 schema
 *       (tracker 14 P1 — CANDLE-SCHEMA-002).</li>
 *   <li>{@code Signal_Candidates} — immutable signal LOG (DEC-035, v3):
 *       <b>no</b> primary key, bucket.key exactly {@code instrument_token},
 *       16 buckets, exact 22-column schema.</li>
 *   <li>{@code Signal_Candidates_current} — KV current-state projection
 *       (DEC-035): primary key exactly {@code [instrument_token]},
 *       bucket.key exactly {@code [instrument_token]} (a subset of the PK, so
 *       per-ticker colocation holds), 16 buckets, the same exact 22-column
 *       schema as the LOG twin (tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001).</li>
 * </ul>
 *
 * <p>Each check compares {@code TableInfo.getSchema().getColumns()} against
 * the shared column contract ({@link CandleTableSchema} or
 * {@link SignalCandidatesTableColumns}) column by column — names in DDL order
 * and Fluss type root per column — so a future DDL that renames, reorders,
 * widens, or re-types a column fails startup instead of silently
 * mis-serializing rows.
 *
 * <p>Nullability enforcement follows what Fluss actually guarantees in live
 * metadata (verified against the dev cluster 2026-08-10): a LOG table reports
 * every column nullable (DDL NOT NULL is not carried into LOG metadata).
 * DDL-level NOT NULL is reported by {@link #schemaReport(TableInfo, List)} as
 * divergence information, not a failure — the storage layer cannot enforce
 * it.
 *
 * <p>Violations fail startup with a {@link ContractViolation} — never a
 * degraded write. The checks are pure {@link TableInfo} inspection so they are
 * unit-testable without a cluster; {@link SignalJob#preflightTableContracts}
 * feeds them from the live cluster before the environment is created.
 */
public final class TableContractValidator {

    /** Thrown when a deployed table violates the contract the write path relies on. */
    public static final class ContractViolation extends IllegalStateException {
        public ContractViolation(String message) {
            super(message);
        }
    }

    private static final String SIGNAL_CONTRACT = "tracker 14 re-scoped P2, SIGNAL-SCHEMA-001";
    private static final String CANDLE_CONTRACT = "tracker 14 P1, CANDLE-SCHEMA-002";

    private TableContractValidator() {}

    /** Candle LOG: append-only, no primary key, instrument_token routing, exact 15-col schema. */
    public static void validateCandleLogTable(TableInfo info) {
        requireNoPrimaryKey(info, "append-only candle LOG", CANDLE_CONTRACT);
        validateSchema(info, CandleTableSchema.COLUMNS, CandleTableSchema.COLUMN_TYPE_ROOTS,
                "15-column v2 candle", CANDLE_CONTRACT);
        validateRouting(info, CANDLE_CONTRACT);
    }

    /** Signal LOG: append-only, no primary key, instrument_token routing, exact 22-col schema. */
    public static void validateSignalLogTable(TableInfo info) {
        requireNoPrimaryKey(info, "append-only signal LOG", SIGNAL_CONTRACT);
        validateSchema(info, signalNames(), SignalCandidatesTableColumns.TYPE_ROOTS,
                "22-column v3 signal", SIGNAL_CONTRACT);
        validateRouting(info, SIGNAL_CONTRACT);
    }

    /** Signal current-state KV: PK exactly [instrument_token], instrument_token routing, exact 22-col schema. */
    public static void validateSignalCurrentKvTable(TableInfo info) {
        List<String> expectedPk = List.of(SignalCandidatesTableColumns.NAMES[
                SignalCandidatesTableColumns.INSTRUMENT_TOKEN]);
        if (!info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (per-ticker current state), but has NO primary key ("
                            + SIGNAL_CONTRACT + ")");
        }
        if (!expectedPk.equals(info.getPrimaryKeys())) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (per-ticker current state), got "
                            + info.getPrimaryKeys() + " (" + SIGNAL_CONTRACT + ")");
        }
        validateSchema(info, signalNames(), SignalCandidatesTableColumns.TYPE_ROOTS,
                "22-column v3 signal", SIGNAL_CONTRACT);
        validateRouting(info, SIGNAL_CONTRACT);
    }

    /**
     * Human-readable schema report for the startup log (and evidence):
     * column name, live type root, live nullability, DDL intent, and a
     * divergence marker where the DDL's NOT NULL is not carried into live
     * metadata. {@code nullableInDdl.get(i)} is {@code true} when the DDL
     * declares column {@code i} nullable (same semantic as
     * {@link CandleTableSchema#COLUMN_NULLABLE_IN_DDL}). Never throws — the
     * strict checks are {@code validate*Table}.
     */
    public static String schemaReport(TableInfo info, List<Boolean> nullableInDdl) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("table ").append(info.getTablePath()).append(" schema=[");
        List<Schema.Column> columns = info.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Schema.Column c = columns.get(i);
            boolean ddlNotNull = i < nullableInDdl.size() && !nullableInDdl.get(i);
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
    private static void requireNoPrimaryKey(TableInfo info, String what, String contractId) {
        if (info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must have NO primary key (" + what
                            + " contract), got " + info.getPrimaryKeys() + " (" + contractId
                            + ")");
        }
    }


    private static void validateSchema(TableInfo info, List<String> expectedNames,
            List<String> expectedTypes, String contract, String contractId) {
        Schema schema = info.getSchema();
        List<Schema.Column> columns = schema.getColumns();
        if (columns.size() != expectedNames.size()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must carry exactly "
                            + expectedNames.size() + " columns (" + contract + " contract), got "
                            + columns.size() + " (" + contractId + ")");
        }
        for (int i = 0; i < columns.size(); i++) {
            Schema.Column column = columns.get(i);
            String actualType = column.getDataType().getTypeRoot().name();
            if (!expectedNames.get(i).equals(column.getName())) {
                throw new ContractViolation(
                        "table " + info.getTablePath() + " column #" + i + " must be named '"
                                + expectedNames.get(i) + "' (DDL order), got '" + column.getName()
                                + "' (" + contractId + ")");
            }
            if (!expectedTypes.get(i).equals(actualType)) {
                throw new ContractViolation(
                        "table " + info.getTablePath() + " column '" + column.getName()
                                + "' must be " + expectedTypes.get(i) + " (" + contract
                                + " contract), got " + actualType + " (" + contractId + ")");
            }
        }
    }
    private static void validateRouting(TableInfo info, String contractId) {
        String bucketKey = CandleTableSchema.BUCKET_KEY;
        if (!info.hasBucketKey()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must set bucket.key explicitly, but uses the default ("
                            + contractId + ")");
        }
        List<String> bucketKeys = info.getBucketKeys();
        if (!List.of(bucketKey).equals(bucketKeys)) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must be distributed by bucket.key exactly ["
                            + bucketKey + "] (per-ticker colocation with the LOG twin), got "
                            + bucketKeys + " (" + contractId + ")");
        }
        int buckets = info.getNumBuckets();
        if (buckets != CandleTableSchema.BUCKET_COUNT) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must have " + CandleTableSchema.BUCKET_COUNT
                            + " buckets (mirrors the LOG twin), got " + buckets
                            + " (" + contractId + ")");
        }
    }

    private static List<String> signalNames() {
        return Arrays.asList(SignalCandidatesTableColumns.NAMES);
    }
}
