package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.EodControllerState;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the per-table EOD retention plan (SCH-23). */
class EodPlannerTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final Duration LIVE_TTL = Duration.ofDays(2);
    private static final Duration SAFETY_FLOOR = Duration.ofDays(7);

    private static final LocalDate D1 = LocalDate.of(2026, 8, 10);
    private static final LocalDate D2 = LocalDate.of(2026, 8, 11);
    private static final LocalDate D3 = LocalDate.of(2026, 8, 12);
    private static final LocalDate D4 = LocalDate.of(2026, 8, 13);

    private static final long NOW = 1_752_000_000_000L;

    private static EodOffloadRecord verified(LocalDate date) {
        EodOffloadRecord r = EodOffloadRecord.initial(date, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW);
        return r.transition(EodControllerState.VERIFIED, NOW);
    }

    private static EodOffloadRecord pending(LocalDate date) {
        return EodOffloadRecord.initial(date, "feature_candles_15s", "2", NOW);
    }

    @Test
    void allVerifiedDaysPlanAgainstTheThreeDayFloor() {
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(verified(D1), verified(D2), verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.allVerified()).isTrue();
        assertThat(plan.earliestUnverifiedDate()).isNull();
        // protected bound = third-most-recent day's source-expiry bound
        assertThat(plan.protectedExpiryBound())
                .isEqualTo(EodRetentionPolicy.sourceExpiryBound(D2, KOLKATA, LIVE_TTL));
    }

    @Test
    void unverifiedDayExtendsTheProtectedBoundBeyondTheFloor() {
        // newest day pending → its source-expiry bound is later than the 3-day floor
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(verified(D1), verified(D2), verified(D3), pending(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.allVerified()).isFalse();
        assertThat(plan.earliestUnverifiedDate()).isEqualTo(D4);
        assertThat(plan.protectedExpiryBound())
                .isEqualTo(EodRetentionPolicy.sourceExpiryBound(D4, KOLKATA, LIVE_TTL))
                .as("an unverified day always extends the protected bound past the floor");
    }

    @Test
    void oldestUnverifiedDayWinsEvenWhenTheFloorIsLater() {
        // oldest day pending → its bound is earlier than the floor; the floor holds
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(pending(D1), verified(D2), verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.earliestUnverifiedDate()).isEqualTo(D1);
        assertThat(plan.protectedExpiryBound())
                .isEqualTo(EodRetentionPolicy.sourceExpiryBound(D2, KOLKATA, LIVE_TTL))
                .as("the protected bound is the later of unverified and floor — never the floor's loss");
    }

    @Test
    void fewerThanThreeDaysFallsBackToTheOldestDay() {
        EodPlanner.Plan plan = EodPlanner.plan(
                List.of(verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW));
        assertThat(plan.allVerified()).isTrue();
        assertThat(plan.protectedExpiryBound())
                .isEqualTo(EodRetentionPolicy.sourceExpiryBound(D3, KOLKATA, LIVE_TTL));
    }

    @Test
    void extensionFiresOnlyWhenMarginCollapsesBelowTheFloor() {
        Instant closeToBound = EodRetentionPolicy.sourceExpiryBound(D2, KOLKATA, LIVE_TTL)
                .minus(Duration.ofHours(6));
        EodPlanner.Plan tight = EodPlanner.plan(
                List.of(verified(D1), verified(D2), verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, Duration.ofDays(30), closeToBound);
        assertThat(tight.requiresExtension()).isTrue();
        assertThat(tight.marginMs()).isLessThan(Duration.ofDays(30).toMillis());

        EodPlanner.Plan roomy = EodPlanner.plan(
                List.of(verified(D1), verified(D2), verified(D3), verified(D4)),
                KOLKATA, LIVE_TTL, Duration.ofHours(1), closeToBound);
        assertThat(roomy.requiresExtension()).isFalse();
        assertThat(roomy.marginMs()).isGreaterThanOrEqualTo(Duration.ofHours(1).toMillis());
    }

    @Test
    void emptyDayListIsRejected() {
        assertThatThrownBy(() -> EodPlanner.plan(
                List.of(), KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodPlanner.plan(
                null, KOLKATA, LIVE_TTL, SAFETY_FLOOR, Instant.ofEpochMilli(NOW)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
