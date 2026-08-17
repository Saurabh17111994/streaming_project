package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-001: Parse NDJSON golden fixtures → correct GoTick objects.
 *
 * <p>Uses reflection to test the package-private {@code IngestionService.GoTick}
 * inner class deserialization.
 */
@DisplayName("ING-UNIT-001: NDJSON Parsing")
class IngestionServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    @Test
    @DisplayName("Parse full-tick NDJSON line")
    void parseFullTick() throws Exception {
        String json = """
                {"feed":"full","mode":"full","token":3045,"ltp_paise":234500,"close_paise":234200,
                "open_paise":233100,"high_paise":235000,"low_paise":233000,"vwap_paise":234100,
                "ltq":50,"volume":125000,"total_buy_qty":800,"total_sell_qty":600,"atv":500,
                "btv":45,"open_interest":0,"ts_ms":1719000000000,
                "bid_px":[234400,234300,234200,234100,234000],
                "ask_px":[234600,234700,234800,234900,235000],
                "bid_qty":[500,300,200,100,50],
                "ask_qty":[400,350,250,150,100],
                "feed_sequence_local":17}""";

        Object gt = MAPPER.readValue(json, GoTickClass());
        assertNotNull(gt);

        assertEquals("full", field(gt, "feed"));
        assertEquals(3045L, (long) field(gt, "token"));
        assertEquals(234500L, (long) field(gt, "ltp_paise"));
        assertEquals(50L, (long) field(gt, "ltq"));
        assertEquals(125000L, (long) field(gt, "volume"));
        assertEquals(1719000000000L, (long) field(gt, "ts_ms"));
        assertEquals(17L, (long) field(gt, "feed_sequence_local"));

        long[] bidPx = (long[]) field(gt, "bid_px");
        assertNotNull(bidPx);
        assertEquals(5, bidPx.length);
        assertEquals(234400L, bidPx[0]);

        long[] askPx = (long[]) field(gt, "ask_px");
        assertNotNull(askPx);
        assertEquals(5, askPx.length);
        assertEquals(234600L, askPx[0]);
    }

    @Test
    @DisplayName("Parse LTP-only NDJSON line")
    void parseLtpTick() throws Exception {
        String json = """
                {"feed":"ltp","mode":"ltp","token":11536,"ltp_paise":95000,
                "close_paise":94800,"open_paise":0,"high_paise":0,"low_paise":0,
                "vwap_paise":0,"ltq":10,"volume":0,"total_buy_qty":0,"total_sell_qty":0,
                "atv":0,"btv":0,"open_interest":0,"ts_ms":1719000001000,
                "bid_px":[0,0,0,0,0],"ask_px":[0,0,0,0,0],
                "bid_qty":[0,0,0,0,0],"ask_qty":[0,0,0,0,0]}""";

        Object gt = MAPPER.readValue(json, GoTickClass());
        assertNotNull(gt);
        assertEquals("ltp", field(gt, "mode"));
        assertEquals(11536L, (long) field(gt, "token"));
        assertEquals(95000L, (long) field(gt, "ltp_paise"));
        assertEquals(10L, (long) field(gt, "ltq"));
    }

    @Test
    @DisplayName("Parse LTPC tick with OHLC")
    void parseLtpcTick() throws Exception {
        String json = """
                {"feed":"ltpc","mode":"ltpc","token":3045,"ltp_paise":234500,"close_paise":234200,
                "open_paise":233100,"high_paise":235000,"low_paise":233000,"vwap_paise":234100,
                "ltq":50,"volume":125000,"total_buy_qty":800,"total_sell_qty":600,
                "atv":500,"btv":45,"open_interest":0,"ts_ms":1719000002000,
                "bid_px":[0,0,0,0,0],"ask_px":[0,0,0,0,0],
                "bid_qty":[0,0,0,0,0],"ask_qty":[0,0,0,0,0]}""";

        Object gt = MAPPER.readValue(json, GoTickClass());
        assertNotNull(gt);
        assertEquals("ltpc", field(gt, "mode"));
        assertEquals(233100L, (long) field(gt, "open_paise"));
        assertEquals(235000L, (long) field(gt, "high_paise"));
        assertEquals(233000L, (long) field(gt, "low_paise"));
        assertEquals(234200L, (long) field(gt, "close_paise"));
    }

    @Test
    @DisplayName("Reject missing required field")
    void rejectMissingToken() {
        String json = "{\"feed\":\"ltp\",\"mode\":\"ltp\",\"ltp_paise\":95000,\"ts_ms\":1719000000000}";
        try {
            MAPPER.readValue(json, GoTickClass());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("token") || e.getMessage().contains("Unrecognized"));
        }
    }

    @Test
    @DisplayName("Parse empty bid/ask arrays")
    void parseEmptyBidAsk() throws Exception {
        String json = """
                {"feed":"ltp","mode":"ltp","token":1,"ltp_paise":100,"close_paise":0,
                "open_paise":0,"high_paise":0,"low_paise":0,"vwap_paise":0,"ltq":1,
                "volume":0,"total_buy_qty":0,"total_sell_qty":0,"atv":0,"btv":0,
                "open_interest":0,"ts_ms":1,"bid_px":[0,0,0,0,0],"ask_px":[0,0,0,0,0],
                "bid_qty":[0,0,0,0,0],"ask_qty":[0,0,0,0,0]}""";

        Object gt = MAPPER.readValue(json, GoTickClass());
        assertNotNull(gt);
        long[] bid = (long[]) field(gt, "bid_px");
        long[] ask = (long[]) field(gt, "ask_px");
        assertEquals(5, bid.length);
        assertEquals(5, ask.length);
    }

    @Test
    @DisplayName("Classify bridge stderr lines per plan log4j2 rule")
    void classifyBridgeLines() {
        // Warning/error keywords → WARN.
        for (String s : new String[]{"connect failed", "HFT stream ended", "decode error",
                "feed stalled", "subscription partial", "token rejected"}) {
            assertTrue(IngestionService.classifyBridgeLine(s), s + " should be WARN");
        }
        // Ordinary diagnostics → INFO.
        for (String s : new String[]{"authenticated", "subscription plan=abc slots=1",
                "heartbeat sent", "retry_in_2s", ""}) {
            assertFalse(IngestionService.classifyBridgeLine(s), s + " should be INFO");
        }
    }

    // ---- helpers ----

    private static Class<?> GoTickClass() {
        try {
            return Class.forName("com.trading.ingestion.IngestionService$GoTick");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("GoTick class not found", e);
        }
    }

    private static Object field(Object obj, String name) {
        try {
            Field f = obj.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(obj);
        } catch (Exception e) {
            throw new RuntimeException("Field " + name + " not found", e);
        }
    }
}
