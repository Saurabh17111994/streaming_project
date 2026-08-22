package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Streaming-3000 T6: the quarantine row layout — DDL 21 column order, the
 * candle-as-JSON evidence bytes, SHA-256 payload hash, bounded detail, and
 * the schema version. The row is what the {@code ingestion_quarantine} LOG
 * sink appends, so column order must match the DDL exactly.
 */
@DisplayName("CandleQuarantine row building (DDL 21 evidence contract)")
class CandleQuarantineRowTest {

    private static final long T0 = 1_749_999_990_000L;

    private static CandleAccumulator acc() {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = 100;
        acc.highPaise = 90; // violated: below open
        acc.lowPaise = 80;
        acc.closePaise = 110;
        acc.volume = 5;
        acc.tickCount = 3;
        return acc;
    }

    @Test
    @DisplayName("the row carries all 10 DDL-21 columns in order")
    void rowColumnsMatchDdl21Order() {
        TimeWindow window = new TimeWindow(T0, T0 + 15_000L);
        GenericRowData row = CandleQuarantine.buildRow(2885L, acc(), window,
                CandleInvariantCheck.Reason.HIGH, "compute-candle-test-id", 1_750_000_000_000L);

        assertEquals(CandleQuarantineColumns.FIELD_COUNT, row.getArity());
        assertEquals("compute-candle-test-id",
                row.getString(CandleQuarantineColumns.QUARANTINE_ID).toString());
        assertEquals("INVALID_CANDLE_HIGH",
                row.getString(CandleQuarantineColumns.REASON).toString());
        assertEquals(2885L, row.getLong(CandleQuarantineColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", row.getString(CandleQuarantineColumns.EXCHANGE).toString());
        assertEquals("TEST", row.getString(CandleQuarantineColumns.SYMBOL).toString());
        assertEquals(1_750_000_000_000L, row.getLong(CandleQuarantineColumns.DETECTED_TS));
        assertEquals(CandleQuarantineColumns.SCHEMA_VERSION,
                row.getString(CandleQuarantineColumns.SCHEMA_VERSION_INDEX).toString());
    }

    @Test
    @DisplayName("raw_payload is the candle as compact JSON; payload_hash is its SHA-256 hex")
    void evidenceAndHash() {
        TimeWindow window = new TimeWindow(T0, T0 + 15_000L);
        GenericRowData row = CandleQuarantine.buildRow(2885L, acc(), window,
                CandleInvariantCheck.Reason.HIGH, "compute-candle-test-id", 1_750_000_000_000L);

        String expected = "{\"instrument_token\":2885"
                + ",\"exchange\":\"NSE\""
                + ",\"symbol\":\"TEST\""
                + ",\"window_start\":" + T0
                + ",\"window_end\":" + (T0 + 15_000L)
                + ",\"open_paise\":100"
                + ",\"high_paise\":90"
                + ",\"low_paise\":80"
                + ",\"close_paise\":110"
                + ",\"volume\":5"
                + ",\"tick_count\":3"
                + ",\"reason\":\"INVALID_CANDLE_HIGH\"}";

        byte[] payload = row.getBinary(CandleQuarantineColumns.RAW_PAYLOAD);
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), payload,
                "the evidence must be the exact deterministic candle JSON bytes");
        assertEquals(CandleQuarantine.sha256Hex(payload),
                row.getString(CandleQuarantineColumns.PAYLOAD_HASH).toString(),
                "payload_hash must be the SHA-256 hex of the evidence bytes");
    }

    @Test
    @DisplayName("detail is bounded and names the violated check")
    void detailIsBoundedAndReadable() {
        TimeWindow window = new TimeWindow(T0, T0 + 15_000L);

        // Short symbol: the full detail fits and names the violated check.
        GenericRowData fits = CandleQuarantine.buildRow(2885L, acc(), window,
                CandleInvariantCheck.Reason.LOW, "compute-candle-test-id", 1_750_000_000_000L);
        String fitsDetail = fits.getString(CandleQuarantineColumns.DETAIL).toString();
        assertTrue(fitsDetail.endsWith("violated=low"),
                "detail names the violated check: " + fitsDetail);
        assertTrue(fitsDetail.contains("window_start=" + T0),
                "detail carries the window identity");

        // Over-long symbol: detail is hard-capped at the 512-char bound.
        CandleAccumulator acc = acc();
        acc.symbol = "LONG-SYMBOL-" + "x".repeat(600);
        GenericRowData capped = CandleQuarantine.buildRow(2885L, acc, window,
                CandleInvariantCheck.Reason.LOW, "compute-candle-test-id", 1_750_000_000_000L);
        String detail = capped.getString(CandleQuarantineColumns.DETAIL).toString();
        assertEquals(512, detail.length(), "detail is capped at the 512-char bound");
        assertTrue(detail.startsWith("token=2885 symbol=LONG-SYMBOL-"),
                "truncation keeps the identity prefix, never the tails");
    }
}
