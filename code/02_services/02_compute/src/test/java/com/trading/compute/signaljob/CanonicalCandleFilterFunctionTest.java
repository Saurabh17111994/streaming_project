package com.trading.compute.signaljob;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.CandleTableSchema;
import com.trading.compute.telemetry.ComputeOtlpEmitter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * KV-boundary canonical filter (tracker 14 P2 — CANDLE-CANONICAL-001): only
 * rows whose algorithm/configuration pair equals the canonical pair exactly
 * may reach the {@code feature_candles_15s_current} upsert; every drop is
 * counted on {@code compute.kv.filtered.noncanonical}.
 */
@DisplayName("CanonicalCandleFilterFunction")
class CanonicalCandleFilterFunctionTest {

    private final CanonicalCandleFilterFunction filter = new CanonicalCandleFilterFunction();

    private static RowData candle(String algorithm, String configuration) {
        GenericRowData row = new GenericRowData(CandleTableColumns.FIELD_COUNT);
        row.setField(CandleTableColumns.INSTRUMENT_TOKEN, 123L);
        row.setField(CandleTableColumns.WINDOW_START, 1_700_000_000_000L);
        row.setField(CandleTableColumns.ALGORITHM_VERSION, StringData.fromString(algorithm));
        row.setField(CandleTableColumns.CONFIGURATION_VERSION, StringData.fromString(configuration));
        return row;
    }

    @Test
    @DisplayName("canonical pair passes through untouched")
    void canonicalPairPasses() {
        assertThat(filter.filter(candle(
                CandleTableSchema.CANONICAL_ALGORITHM_VERSION,
                CandleTableSchema.CANONICAL_CONFIGURATION_VERSION))).isTrue();
    }

    @Test
    @DisplayName("algorithm deviation is dropped and counted")
    void algorithmDeviationDropped() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        long before = emitter.drainKvFilteredDelta();

        assertThat(filter.filter(candle("candle-15s-v2", "1.0.0"))).isFalse();
        assertThat(emitter.drainKvFilteredDelta()).isEqualTo(before + 1);
    }

    @Test
    @DisplayName("configuration deviation is dropped and counted")
    void configurationDeviationDropped() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        long before = emitter.drainKvFilteredDelta();

        assertThat(filter.filter(candle("candle-15s-v1", "2.0.0"))).isFalse();
        assertThat(emitter.drainKvFilteredDelta()).isEqualTo(before + 1);
    }

    @Test
    @DisplayName("blank and padded versions are dropped (exact-match policy, no trimming)")
    void blankAndPaddedVersionsDropped() {
        ComputeOtlpEmitter emitter = new ComputeOtlpEmitter("localhost:4318");
        long before = emitter.drainKvFilteredDelta();

        assertThat(filter.filter(candle("", "1.0.0"))).isFalse();
        assertThat(filter.filter(candle("candle-15s-v1", ""))).isFalse();
        // a padded value is a different version string, not an equivalent one
        assertThat(filter.filter(candle("candle-15s-v1 ", "1.0.0"))).isFalse();
        assertThat(emitter.drainKvFilteredDelta()).isEqualTo(before + 3);
    }
}
