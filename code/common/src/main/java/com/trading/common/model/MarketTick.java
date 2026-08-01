package com.trading.common.model;

/**
 * Normalized market tick — what the Ingestion writes to raw_table_1.
 * All prices in integer paise (₹1 = 100 paise).
 * Original broker bytes preserved in rawPayload.
 */
public record MarketTick(
    String eventFingerprint,
    String fingerprintVersion,
    String connectionId,
    long connectionEpoch,
    long instrumentToken,
    String exchange,
    String symbol,
    String instrumentType,
    Long strikePaise,            // null for non-options
    Long expiry,
    String optionType,
    long eventTime,
    long ingestTs,
    long ackTs,
    String tickType,
    Long lastPricePaise,
    Long lastQty,
    Long bidPricePaise,
    Long bidQty,
    Long askPricePaise,
    Long askQty,
    byte[] rawPayload,
    String payloadHash,
    String decoderVersion,
    String protocolVersion,
    String validityState,
    String validityReason,
    String schemaVersion
) {
    public boolean isTrade() { return "TRADE".equals(tickType); }
    public boolean isValid() { return "VALID".equals(validityState); }
    public boolean isValidTrade() { return isTrade() && isValid(); }
}
