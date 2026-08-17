package com.trading.common.model;

/**
 * Immutable trade instruction — the Signal job's final output.
 * Consumed by the Executor. Never mutated after creation.
 * Prices in integer paise (₹1 = 100 paise). compositeScore is a ratio (double).
 */
public record TradeDecision(
    String instructionId,
    String candidateId,
    String tradeContextId,
    long instrumentToken,
    String symbol,
    String exchange,
    String side,                 // BUY, SELL
    long quantity,
    String product,
    String orderType,
    Long limitPricePaise,        // null for MARKET orders; integer paise
    String strategy,
    String strategyVersion,
    double compositeScore,       // score — not monetary
    String rankingModelVersion,
    String reservationId,
    String reservationVersion,
    long creationTimestamp,
    long expiryTimestamp,
    String supersedesInstructionId,
    String schemaVersion
) {}
