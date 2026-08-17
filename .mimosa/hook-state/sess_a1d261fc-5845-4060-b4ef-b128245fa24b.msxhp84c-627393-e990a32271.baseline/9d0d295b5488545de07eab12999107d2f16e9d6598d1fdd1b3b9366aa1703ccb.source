package com.trading.common.model;

/**
 * Candidate signal record — produced by Business Logic.
 * A candidate_id is always created; instruction_id is null until selected by Ranking.
 * Prices in integer paise (₹1 = 100 paise). Scores are ratios (double).
 */
public record SignalCandidate(
    String candidateId,
    String instructionId,        // null until Ranking selects
    String tradeContextId,       // null if new entry
    long instrumentToken,
    String strategy,
    String rule,
    String action,               // BUY, SELL
    long pricePaise,
    long quantity,
    String product,
    String orderType,
    long eventTimestamp,
    long evaluationTimestamp,
    double scoreConfidence,      // score — not monetary
    double scoreRiskReward,      // score — not monetary
    double scoreExpectedMove,    // score — not monetary
    String strategyVersion,
    String detectionReason,
    String validityState,
    String supersedesCandidateId,
    String schemaVersion
) {}
