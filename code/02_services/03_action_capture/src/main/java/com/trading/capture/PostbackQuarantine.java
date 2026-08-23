package com.trading.capture;

/**
 * Simple immutable quarantine entry for rejected postbacks (pure logic, no Fluss).
 */
public final class PostbackQuarantine {

    private PostbackQuarantine() {}

    /**
     * Immutable quarantine entry.
     *
     * @param postbackEventId event id (fingerprint) of the quarantined postback
     * @param reason          quarantine reason code (e.g. fingerprint_mismatch, unknown_order_update)
     * @param rawJson         original raw payload as JSON string (may be empty)
     * @param quarantinedTs   wall-clock quarantine timestamp in epoch millis
     */
    public record QuarantineEntry(
            String postbackEventId,
            String reason,
            String rawJson,
            long quarantinedTs) {}

    /**
     * Factory for a quarantine entry timestamped at creation.
     *
     * @param eventId postback event id
     * @param reason  quarantine reason
     * @param raw     raw JSON payload
     * @return new entry with {@code System.currentTimeMillis()} as timestamp
     */
    public static QuarantineEntry quarantine(String eventId, String reason, String raw) {
        return new QuarantineEntry(eventId, reason, raw, System.currentTimeMillis());
    }
}
