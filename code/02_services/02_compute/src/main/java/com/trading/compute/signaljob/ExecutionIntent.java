package com.trading.compute.signaljob;

/** Immutable execution request before any broker-facing processing exists. */
public record ExecutionIntent(
        String instructionId,
        String candidateId,
        String tradeContextId,
        String accountScopeId,
        String executionPartitionId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        long quantity,
        String orderType,
        Long limitPricePaise,
        String productType,
        String timeInForce,
        String strategyId,
        String strategyVersion,
        String configurationVersion,
        long createdTs,
        Long expiryTs,
        String supersedesInstructionId) {}
