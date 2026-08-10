package com.trading.compute.signaljob;

import com.trading.common.schema.CandleTableSchema;
import java.util.List;
import org.apache.fluss.metadata.TableInfo;

/**
 * Read-only candle-table contract checks (CANDLE-KV-REPLAY-001 P4/A4.x).
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

    /** LOG twin: append-only, no primary key, instrument_token bucketing. */
    public static void validateLogTable(TableInfo info) {
        if (info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "LOG table " + info.getTablePath() + " must have NO primary key (append-only), "
                            + "but is a KV table with PK " + info.getPrimaryKeys()
                            + " (CANDLE-KV-REPLAY-001 P4)");
        }
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
        validateRouting(info);
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
