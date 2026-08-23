package com.trading.capture;

import java.util.Map;

/**
 * Pure static decoder mirroring {@code go-bridge/postback.go#NormalizeOrderUpdate}.
 *
 * <p>Extracts the broker postback payload into a typed record without any
 * Fluss or broker dependency. All string extraction is null-safe and
 * trims whitespace, mirroring Go's {@code stringField} helper which returns
 * {@code ""} for missing/null, {@code TrimSpace} for strings, and
 * {@code fmt.Sprint} for numbers.
 */
public final class PostbackDecoder {

    private PostbackDecoder() {}

    /**
     * Decode a raw broker postback map (e.g. parsed JSON object) into a typed envelope.
     *
     * <p>Field mapping (pure logic, no Fluss):
     * <ul>
     *   <li>{@code brokerOrderId} ← {@code raw.get("id")}</li>
     *   <li>{@code clientOrderRef} ← {@code raw.get("remarks")}</li>
     *   <li>{@code orderStatus} ← {@code raw.get("orderStatus")}</li>
     *   <li>{@code reportType} ← {@code raw.get("reportType")}</li>
     *   <li>{@code fillShares} ← {@code raw.get("fillShares")}</li>
     *   <li>{@code averagePrice} ← {@code raw.get("averagePrice")}</li>
     *   <li>{@code fillPrice} ← {@code raw.get("fillPrice")}</li>
     *   <li>{@code fillQuantity} ← {@code raw.get("fillQuantity")}</li>
     *   <li>{@code fillTime} ← {@code raw.get("fillTime")}</li>
     *   <li>{@code instrumentToken} ← {@code raw.get("token")}</li>
     *   <li>{@code exchangeUpdateTime} ← {@code raw.get("exchangeUpdateTime")}</li>
     *   <li>{@code receivedTsMs} ← {@code System.currentTimeMillis()}</li>
     * </ul>
     *
     * @param raw raw postback payload (may be null)
     * @return decoded postback
     */
    public static DecodedPostback decode(Map<String, Object> raw) {
        if (raw == null) {
            raw = Map.of();
        }
        String brokerOrderId = stringField(raw, "id");
        String clientOrderRef = stringField(raw, "remarks");
        String orderStatus = stringField(raw, "orderStatus");
        String reportType = stringField(raw, "reportType");
        String fillShares = stringField(raw, "fillShares");
        String averagePrice = stringField(raw, "averagePrice");
        String fillPrice = stringField(raw, "fillPrice");
        String fillQuantity = stringField(raw, "fillQuantity");
        String fillTime = stringField(raw, "fillTime");
        String instrumentToken = stringField(raw, "token");
        String exchangeUpdateTime = stringField(raw, "exchangeUpdateTime");
        long receivedTsMs = System.currentTimeMillis();
        return new DecodedPostback(
                brokerOrderId,
                clientOrderRef,
                orderStatus,
                reportType,
                fillShares,
                averagePrice,
                fillPrice,
                fillQuantity,
                fillTime,
                instrumentToken,
                exchangeUpdateTime,
                receivedTsMs);
    }

    private static String stringField(Map<String, Object> raw, String key) {
        Object value = raw.get(key);
        if (value == null) {
            return "";
        }
        if (value instanceof String s) {
            return s.trim();
        }
        // Handle Number (Integer, Long, Double, etc.) and any other type via toString.
        // Mirrors Go: json.Number -> String(), float64 -> %g, default -> Sprint, all trimmed.
        return String.valueOf(value).trim();
    }

    /**
     * Immutable decoded postback capturing the raw broker fields in normalized string form.
     *
     * @param brokerOrderId      broker order id (raw {@code id})
     * @param clientOrderRef     client order ref (raw {@code remarks})
     * @param orderStatus        broker order status
     * @param reportType         broker report type
     * @param fillShares         fill shares (string, may be empty)
     * @param averagePrice       average fill price (string, may be empty)
     * @param fillPrice          fill price (string, may be empty)
     * @param fillQuantity       fill quantity (string, may be empty)
     * @param fillTime           fill time (string, may be empty)
     * @param instrumentToken    instrument token (raw {@code token})
     * @param exchangeUpdateTime exchange update time (raw {@code exchangeUpdateTime})
     * @param receivedTsMs       wall-clock receive time in epoch millis
     */
    public record DecodedPostback(
            String brokerOrderId,
            String clientOrderRef,
            String orderStatus,
            String reportType,
            String fillShares,
            String averagePrice,
            String fillPrice,
            String fillQuantity,
            String fillTime,
            String instrumentToken,
            String exchangeUpdateTime,
            long receivedTsMs) {}
}
