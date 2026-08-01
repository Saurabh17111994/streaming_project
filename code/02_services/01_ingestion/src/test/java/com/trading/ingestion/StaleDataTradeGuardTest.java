package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 1376 — Stale data cannot create a trade decision.
 *
 * <p>Uses the evidence-backed production freshness values
 * (ARROW_MAX_EVENT_AGE_MS=5000, ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000).
 * A tick whose broker timestamp is stale or too-far-future is classified
 * before any trade path: the IngestionService quarantines it, so it can
 * never become a candle/trade input.
 */
@DisplayName("1376: stale data cannot create a trade decision")
class StaleDataTradeGuardTest {

    private static final long MAX_AGE_MS = 5000;
    private static final long MAX_FUTURE_SKEW_MS = 2000;
    private static final long RECEIVE = 1_000_000_000L;

    @Test
    @DisplayName("fresh timestamp passes the gate")
    void freshTimestampAllowed() {
        assertEquals(IngestionService.FreshnessDecision.FRESH,
                IngestionService.classifyFreshness(RECEIVE - 100, RECEIVE, MAX_FUTURE_SKEW_MS, MAX_AGE_MS));
    }

    @Test
    @DisplayName("event at exactly max age is still fresh (boundary inclusive)")
    void boundaryAgeAllowed() {
        assertEquals(IngestionService.FreshnessDecision.FRESH,
                IngestionService.classifyFreshness(RECEIVE - 5000, RECEIVE, MAX_FUTURE_SKEW_MS, MAX_AGE_MS));
    }

    @Test
    @DisplayName("event older than max age is STALE")
    void staleTimestampQuarantined() {
        assertEquals(IngestionService.FreshnessDecision.STALE,
                IngestionService.classifyFreshness(RECEIVE - 5001, RECEIVE, MAX_FUTURE_SKEW_MS, MAX_AGE_MS));
    }

    @Test
    @DisplayName("event far in the future is FUTURE")
    void futureTimestampQuarantined() {
        assertEquals(IngestionService.FreshnessDecision.FUTURE,
                IngestionService.classifyFreshness(RECEIVE + 2001, RECEIVE, MAX_FUTURE_SKEW_MS, MAX_AGE_MS));
    }

    @Test
    @DisplayName("event at exactly max future skew is allowed (boundary inclusive)")
    void boundaryFutureAllowed() {
        assertEquals(IngestionService.FreshnessDecision.FRESH,
                IngestionService.classifyFreshness(RECEIVE + 2000, RECEIVE, MAX_FUTURE_SKEW_MS, MAX_AGE_MS));
    }
}
