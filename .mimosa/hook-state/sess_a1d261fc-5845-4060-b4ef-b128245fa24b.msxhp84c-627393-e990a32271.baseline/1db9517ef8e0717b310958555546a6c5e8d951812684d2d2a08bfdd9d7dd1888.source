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

    @Test
    @DisplayName("ING-UNIT-014: exact boundary matrix — receive==broker time, ±1 ms over/under both limits")
    void exactBoundaryMatrix() {
        // Walk EVERY boundary: receive == broker time, one ms under/over
        // maxEventAge, one ms under/over maxFutureSkew. The stale check is
        // `receive - ts > maxAge` (age inclusive); the future check is
        // `ts - receive > maxSkew` (skew inclusive) — both exact-boundary
        // values are FRESH, one ms beyond flips the class.
        record Case(long tsMs, IngestionService.FreshnessDecision want) {}
        java.util.List<Case> cases = java.util.List.of(
                new Case(RECEIVE, IngestionService.FreshnessDecision.FRESH),                       // receive == broker time
                new Case(RECEIVE - MAX_AGE_MS + 1, IngestionService.FreshnessDecision.FRESH),      // 1 ms under max age
                new Case(RECEIVE - MAX_AGE_MS, IngestionService.FreshnessDecision.FRESH),          // exactly max age (inclusive)
                new Case(RECEIVE - MAX_AGE_MS - 1, IngestionService.FreshnessDecision.STALE),      // 1 ms over → STALE
                new Case(RECEIVE + MAX_FUTURE_SKEW_MS - 1, IngestionService.FreshnessDecision.FRESH),   // 1 ms under skew
                new Case(RECEIVE + MAX_FUTURE_SKEW_MS, IngestionService.FreshnessDecision.FRESH),       // exactly skew (inclusive)
                new Case(RECEIVE + MAX_FUTURE_SKEW_MS + 1, IngestionService.FreshnessDecision.FUTURE)   // 1 ms over → FUTURE
        );
        for (Case c : cases) {
            assertEquals(c.want(),
                    IngestionService.classifyFreshness(c.tsMs(), RECEIVE, MAX_FUTURE_SKEW_MS, MAX_AGE_MS),
                    "tsMs=" + c.tsMs() + " receive=" + RECEIVE);
        }
    }
}
