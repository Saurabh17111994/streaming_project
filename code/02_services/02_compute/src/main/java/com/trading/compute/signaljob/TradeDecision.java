package com.trading.compute.signaljob;

/**
 * Typed executable request the Signal-job decision builder turns into one
 * immutable {@code Trade_Decisions} LOG row (REQ-FLS-008 / REQ-SS-004 /
 * {@code docs/04_contracts/10-ranking.md}).
 *
 * <p>{@code instruction_id} is deliberately NOT an input: it is derived
 * deterministically from the executable identity by
 * {@link TradeDecisionBuilder} (REQ-SS-004 — the Signal job SHALL not reuse
 * an {@code instruction_id} for different quantity, side, symbol, price,
 * strategy version, or trade context; identical content MUST yield the same
 * id so a same-winner unchanged-parameter re-evaluation is audit-only).
 *
 * <p>Contains no Executor-assigned fields by construction (no
 * {@code client_order_ref}, {@code broker_order_id}, no execution status).
 *
 * @param candidateId immutable candidate identity that won the evaluation
 * @param tradeContextId stable trade-context identity
 * @param instrumentToken routing/execution instrument
 * @param exchange exchange the instrument trades on
 * @param symbol instrument symbol
 * @param side BUY or SELL ({@link TradeDecisionsTableColumns#SIDE_BUY})
 * @param quantity non-positive rejected
 * @param orderType MARKET or LIMIT
 * @param productType product class
 * @param limitPricePaise null for market orders; non-null (> 0) for limit
 * @param portfolioId ranking/capacity scope
 * @param accountScopeId account scope
 * @param strategyId strategy provenance
 * @param strategyVersion strategy provenance (part of the executable identity)
 * @param configurationVersion configuration provenance
 * @param evaluationId ranking evaluation that produced this winner
 * @param compositeScore null or finite score from the evaluation
 * @param reservationId reservation backing this instruction
 * @param reservationVersion reservation transition version
 * @param createdTs emission timestamp (epoch millis; must be positive)
 * @param expiryTs null unless the instruction carries an explicit expiry
 * @param supersedesInstructionId set when this instruction replaces a prior one
 * @param supersededByInstructionId informational only — the LOG row is
 *        append-only, so this is practically null at emission time
 */
public record TradeDecision(
        String candidateId,
        String tradeContextId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        long quantity,
        String orderType,
        String productType,
        Long limitPricePaise,
        String portfolioId,
        String accountScopeId,
        String strategyId,
        String strategyVersion,
        String configurationVersion,
        String evaluationId,
        Double compositeScore,
        String reservationId,
        String reservationVersion,
        long createdTs,
        Long expiryTs,
        String supersedesInstructionId,
        String supersededByInstructionId) {
}
