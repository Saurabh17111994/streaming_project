package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.common.schema.CandleTableSchema;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Drift guard between the compute column layout and the shared schema
 * (CANDLE-KV-REPLAY-001 P1/A4.1): {@code CandleTableColumns} must stay a
 * faithful mirror of {@link CandleTableSchema#COLUMNS} in both name order and
 * count — the candle KV sink and the migration tool both serialize against
 * this layout.
 */
@DisplayName("CandleTableColumns mirrors CandleTableSchema.COLUMNS")
class CandleTableColumnsAgreementTest {

    @Test
    @DisplayName("NAMES equals the shared schema columns in order")
    void namesEqualSharedColumns() {
        List<String> shared = CandleTableSchema.COLUMNS;
        assertArrayEquals(shared.toArray(new String[0]), CandleTableColumns.NAMES,
                "column names/order must match CandleTableSchema.COLUMNS");
        assertEquals(shared.size(), CandleTableColumns.FIELD_COUNT);
        assertEquals(15, CandleTableColumns.FIELD_COUNT,
                "the shared 15-column candle layout is fixed by the DDL");
    }

    @Test
    @DisplayName("index constants point at the expected shared columns")
    void indexConstantsMatchSharedColumns() {
        assertEquals(CandleTableSchema.COLUMNS.indexOf("instrument_token"), CandleTableColumns.INSTRUMENT_TOKEN);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("exchange"), CandleTableColumns.EXCHANGE);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("symbol"), CandleTableColumns.SYMBOL);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("window_start"), CandleTableColumns.WINDOW_START);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("window_end"), CandleTableColumns.WINDOW_END);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("open_paise"), CandleTableColumns.OPEN_PAISE);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("high_paise"), CandleTableColumns.HIGH_PAISE);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("low_paise"), CandleTableColumns.LOW_PAISE);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("close_paise"), CandleTableColumns.CLOSE_PAISE);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("volume"), CandleTableColumns.VOLUME);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("tick_count"), CandleTableColumns.TICK_COUNT);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("algorithm_version"), CandleTableColumns.ALGORITHM_VERSION);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("configuration_version"), CandleTableColumns.CONFIGURATION_VERSION);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("output_ts"), CandleTableColumns.OUTPUT_TS);
        assertEquals(CandleTableSchema.COLUMNS.indexOf("schema_version"), CandleTableColumns.SCHEMA_VERSION);
        // sanity: every index constant is a distinct, valid position
        int[] idx = {CandleTableColumns.INSTRUMENT_TOKEN, CandleTableColumns.EXCHANGE,
                CandleTableColumns.SYMBOL, CandleTableColumns.WINDOW_START, CandleTableColumns.WINDOW_END,
                CandleTableColumns.OPEN_PAISE, CandleTableColumns.HIGH_PAISE, CandleTableColumns.LOW_PAISE,
                CandleTableColumns.CLOSE_PAISE, CandleTableColumns.VOLUME, CandleTableColumns.TICK_COUNT,
                CandleTableColumns.ALGORITHM_VERSION, CandleTableColumns.CONFIGURATION_VERSION,
                CandleTableColumns.OUTPUT_TS, CandleTableColumns.SCHEMA_VERSION};
        assertEquals(15, Arrays.stream(idx).distinct().count(), "index constants must be pairwise distinct");
        for (int i : idx) {
            assertEquals(0, i >= 0 && i < CandleTableColumns.FIELD_COUNT ? 0 : 1, "index " + i + " out of range");
        }
    }
}
