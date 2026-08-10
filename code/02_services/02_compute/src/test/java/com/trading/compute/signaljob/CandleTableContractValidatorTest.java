package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
 * Candle-table contract validator (CANDLE-KV-REPLAY-001 P4/A4.x): the LOG
 * twin must be append-only and the KV twin must carry exactly the canonical
 * PK with the same routing, or startup fails closed.
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
     */
    private static TableInfo table(String name, List<String> schemaPk, List<String> ignoredPk,
            List<String> bucketKeys, int numBuckets) {
        Schema.Builder sb = Schema.newBuilder();
        for (String col : CandleTableSchema.COLUMNS) {
            sb.column(col, DataTypes.BIGINT());
        }
        if (schemaPk != null) {
            sb.primaryKey(schemaPk);
        }
        return new TableInfo(TablePath.of("default", name), 1L, 1, sb.build(), bucketKeys,
                List.of(), numBuckets, new Configuration(), new Configuration(), null, 0L, 0L);
    }
}
