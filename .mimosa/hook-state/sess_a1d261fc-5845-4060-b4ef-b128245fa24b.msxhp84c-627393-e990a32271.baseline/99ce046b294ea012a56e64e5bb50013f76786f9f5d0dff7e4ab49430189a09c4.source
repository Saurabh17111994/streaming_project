package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.EodControllerState;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/** Unit tests for the durable per-day offload record + its state machine (SCH-23). */
class EodOffloadRecordTest {

    private static final long NOW = 1_752_000_000_000L;
    private static final LocalDate DAY = LocalDate.of(2026, 8, 14);

    @Test
    void initialRecordIsPendingWithSourceExpiryNever() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThat(r.tradingDate()).isEqualTo("2026-08-14");
        assertThat(r.state()).isEqualTo(EodControllerState.PENDING);
        assertThat(r.retryCount()).isZero();
        assertThat(r.nextRetryAtMs()).isZero();
        assertThat(r.earliestAllowedSourceExpiryMs()).isEqualTo(Long.MAX_VALUE);
        assertThat(r.permitsSourceExpiry()).isFalse();
        assertThat(r.requiresRetentionExtension())
                .as("a PENDING day must extend live retention").isTrue();
    }

    @Test
    void happyPathReachesVerifiedAndReleasesSourceExpiry() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW)
                .transition(EodControllerState.VERIFIED, NOW);
        assertThat(r.state()).isEqualTo(EodControllerState.VERIFIED);
        assertThat(r.earliestAllowedSourceExpiryMs()).isEqualTo(NOW);
        assertThat(r.permitsSourceExpiry()).isTrue();
        assertThat(r.requiresRetentionExtension()).isFalse();
        assertThat(r.nextRetryAtMs()).isZero();
    }

    @Test
    void retryableFailureIncrementsRetryCountAndSchedulesBackoff() {
        EodOffloadRecord failed = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_RETRYABLE, NOW);
        assertThat(failed.state()).isEqualTo(EodControllerState.FAILED_RETRYABLE);
        assertThat(failed.retryCount()).isEqualTo(1);
        assertThat(failed.nextRetryAtMs()).isGreaterThan(NOW);
        assertThat(failed.nextRetryAtMs() - NOW).isBetween(1_600L, 2_399L);
        assertThat(failed.permitsSourceExpiry())
                .as("a retryable day must never permit source expiry").isFalse();

        // retry clears the schedule; a second failure backoffs harder
        EodOffloadRecord retrying = failed.transition(EodControllerState.WRITING, NOW);
        assertThat(retrying.nextRetryAtMs()).isZero();
        EodOffloadRecord failedAgain = retrying
                .transition(EodControllerState.FAILED_RETRYABLE, NOW);
        assertThat(failedAgain.retryCount()).isEqualTo(2);
        assertThat(failedAgain.nextRetryAtMs() - NOW).isBetween(3_200L, 4_799L);
    }

    @Test
    void manualFailureRequiresExplicitResetToPending() {
        EodOffloadRecord manual = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW)
                .transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.FAILED_MANUAL, NOW);
        assertThat(manual.state()).isEqualTo(EodControllerState.FAILED_MANUAL);
        assertThat(manual.requiresRetentionExtension()).isTrue();
        EodOffloadRecord reset = manual.transition(EodControllerState.PENDING, NOW);
        assertThat(reset.state()).isEqualTo(EodControllerState.PENDING);
    }

    @Test
    void illegalAndRegressiveTransitionsThrow() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThatThrownBy(() -> r.transition(EodControllerState.COMMITTED, NOW))
                .as("PENDING → COMMITTED skips WRITING").isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> r.transition(EodControllerState.VERIFIED, NOW))
                .as("PENDING → VERIFIED skips the pipeline").isInstanceOf(IllegalStateException.class);

        EodOffloadRecord verified = r.transition(EodControllerState.WRITING, NOW)
                .transition(EodControllerState.COMMITTED, NOW)
                .transition(EodControllerState.VERIFYING, NOW)
                .transition(EodControllerState.VERIFIED, NOW);
        assertThatThrownBy(() -> verified.transition(EodControllerState.PENDING, NOW))
                .as("a VERIFIED day must never silently regress")
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> verified.transition(EodControllerState.FAILED_RETRYABLE, NOW))
                .as("VERIFIED is terminal").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void legalTransitionMapCoversTheMachineEdges() {
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.PENDING,
                EodControllerState.WRITING)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.VERIFYING,
                EodControllerState.VERIFIED)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.FAILED_RETRYABLE,
                EodControllerState.WRITING)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.FAILED_MANUAL,
                EodControllerState.PENDING)).isTrue();
        assertThat(EodOffloadRecord.isLegalTransition(EodControllerState.VERIFIED,
                EodControllerState.PENDING)).isFalse();
    }

    @Test
    void dateFormattingRoundTrips() {
        EodOffloadRecord r = EodOffloadRecord.initial(DAY, "feature_candles_15s", "2", NOW);
        assertThat(r.tradingDateAsLocalDate()).isEqualTo(DAY);
        assertThat(EodOffloadRecord.parseTradingDate(EodOffloadRecord.formatTradingDate(DAY)))
                .isEqualTo(DAY);
    }
}
