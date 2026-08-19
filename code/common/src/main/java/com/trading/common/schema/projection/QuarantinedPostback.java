package com.trading.common.schema.projection;

/** Row mirror of Postback_Quarantine (16_postback_quarantine.sql). */
public record QuarantinedPostback(
        String quarantineId,
        String postbackEventId,
        QuarantineReason reason,
        byte[] originalPayload,
        String payloadHash,
        String brokerOrderId,
        String instructionId,
        String correlationAttempt,
        String disposition,
        String dispositionReason,
        long quarantinedTs,
        Long dispositionTs,
        String schemaVersion) {
}
