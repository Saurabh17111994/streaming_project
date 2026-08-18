package com.trading.execution.gateway;

/** Gateway-local copy of the immutable Execution_Intent wire contract. */
public record IntentRecord(
        String instructionId, String candidateId, String tradeContextId, String accountScopeId,
        String executionPartitionId, long instrumentToken, String exchange, String symbol,
        String side, long quantity, String orderType, Long limitPricePaise, String productType,
        String timeInForce, String strategyId, String strategyVersion, String configurationVersion,
        long createdTs, Long expiryTs, String requestHash, String supersedesInstructionId,
        String schemaVersion, long sourceOffset) {}
