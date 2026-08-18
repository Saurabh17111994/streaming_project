package com.trading.execution.gateway;

/** Normalized Nautilus event. Values are already authoritative images; the gateway does not derive them. */
public record NormalizedExecutionEvent(
        String postbackEventId, String accountScopeId, String executionPartitionId,
        long gateEpoch, String actorId, String eventType, long eventTs,
        Audit audit, Fill fill, Lifecycle lifecycle, Position position, Correlation correlation) {
    public record Audit(String auditEventId, String evidenceHash, String evidenceSummary) {}
    public record Fill(String postbackFingerprint, String fingerprintVersion, String brokerOrderId,
                       String instructionId, String executionAttemptId, String tradeContextId,
                       String orderStatus, long cumulativeQty, long pendingQty, Long fillQty,
                       Long fillPricePaise, String fillId, Long brokerEventTime, long receiveTime,
                       long ingestTs, byte[] originalPayload, String payloadHash,
                       String correlationState, String correlationReason, String decoderVersion) {}
    public record Lifecycle(String brokerOrderId, String instructionId, String executionAttemptId,
                            String tradeContextId, String normalizedState, long cumulativeQty,
                            long pendingQty, Long averageFillPricePaise, long sourceVersion,
                            Long sourceEventTime, long lastReceiveTime, String correlationState) {}
    public record Position(String positionId, String tradeContextId, long instrumentToken,
                           String exchange, String symbol, String side, String state,
                           long openQuantity, long closedQuantity, Long averageEntryPaise,
                           Long averageExitPaise, long sourceVersion, long createdTs, long lastUpdateTs) {}
    public record Correlation(String instructionId, String executionAttemptId, String clientOrderRef,
                              String brokerOrderId, String tradeContextId, String positionId,
                              String verificationState, String verificationEvidence, long correlatedTs) {}
}
