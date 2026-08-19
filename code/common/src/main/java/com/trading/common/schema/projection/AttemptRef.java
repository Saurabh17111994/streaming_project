package com.trading.common.schema.projection;

/** The correlation identity of a placed order — what a registry lookup resolves. */
public record AttemptRef(
        String accountScopeId,
        String instructionId,
        String executionAttemptId,
        String tradeContextId) {

    public AttemptRef {
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("accountScopeId is required");
        }
        if (instructionId == null || instructionId.isBlank()) {
            throw new IllegalArgumentException("instructionId is required");
        }
    }
}
