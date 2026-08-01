package com.trading.common.model;

/**
 * Ranked result for a single candidate within one evaluation.
 */
public record RankingResult(
    String evaluationId,
    String candidateId,
    String instructionId,        // null if not selected
    String rankingModelVersion,
    String configHash,
    double normalizedScore,
    double confidenceComponent,
    double riskRewardComponent,
    double expectedMoveComponent,
    int rank,
    boolean selected,
    String rejectionReason,
    String reservationSnapshotVersion,
    long evaluationTimestamp,
    String schemaVersion
) {}
