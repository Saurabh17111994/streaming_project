package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Schema/validity gate decisions — pure classification, no runtime context needed. */
class RawValidationFunctionTest {

    private RawValidationFunction fn;

    @BeforeEach
    void setUp() {
        Map<String, String> env = new java.util.HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        fn = new RawValidationFunction(SignalJobConfig.from(env));
    }

    @Test
    void acceptsValidTradeRow() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertNull(fn.invalidReason(row));
    }

    @Test
    void acceptsValidNonTradeQuoteRow() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "QUOTE", 100, 0);
        assertNull(fn.invalidReason(row));
    }

    @Test
    void rejectsInvalidValidityState() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("validity-state", fn.invalidReason(TestRawRows.withValidity(row, "INVALID_VALUES")));
    }

    @Test
    void rejectsNonPositivePrice() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("non-positive-price", fn.invalidReason(TestRawRows.withPrice(row, 0)));
        assertEquals("non-positive-price", fn.invalidReason(TestRawRows.withPrice(row, -12)));
    }

    @Test
    void rejectsNegativeQuantity() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("negative-qty", fn.invalidReason(TestRawRows.withQty(row, -1)));
    }

    @Test
    void rejectsUnknownSchemaVersion() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("schema-version", fn.invalidReason(TestRawRows.withSchemaVersion(row, "3")));
    }

    @Test
    void rejectsLegacyV1LabelUnderDefaultConfig() {
        // Pre-fix ingestion labeled the v2-shaped row "1" (TickPacket.schemaVersion=1).
        // Under the v2-only contract that label stays rejected — no silent acceptance
        // of a label that would mask a future producer regression.
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("schema-version", fn.invalidReason(TestRawRows.withSchemaVersion(row, "1")));
    }

    @Test
    void rejectsNonInsertRowKind() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        ((org.apache.flink.table.data.GenericRowData) row).setRowKind(RowKind.UPDATE_AFTER);
        assertEquals("rowkind-not-insert", fn.invalidReason(row));
    }
}
