package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import org.junit.jupiter.api.Test;

/** Unit tests for the EOD controller retry/backoff schedule (SCH-23). */
class EodBackoffTest {

    private static final Random RNG = new Random(42L);

    @Test
    void defaultsAreOneSecondBaseAndFiveMinuteCap() {
        assertThat(EodBackoff.DEFAULT_BASE_MS).isEqualTo(1_000L);
        assertThat(EodBackoff.DEFAULT_MAX_MS).isEqualTo(300_000L);
    }

    @Test
    void delayGrowsExponentiallyUntilTheCap() {
        // Seeded RNG makes the sequence deterministic; jitter never bridges the
        // exponential gap (factor 2 vs jitter range [0.8, 1.2)).
        long first = EodBackoff.nextRetryDelayMs(0, 1_000L, 300_000L, RNG);
        long second = EodBackoff.nextRetryDelayMs(1, 1_000L, 300_000L, RNG);
        long third = EodBackoff.nextRetryDelayMs(2, 1_000L, 300_000L, RNG);
        assertThat(second).isGreaterThan(first);
        assertThat(third).isGreaterThan(second);
    }

    @Test
    void delayIsJitteredWithinTwentyPercentOfTheBackoffStep() {
        long delay = EodBackoff.nextRetryDelayMs(0, 1_000L, 300_000L, RNG);
        assertThat(delay).isBetween(800L, 1_199L); // [0.8 * base, 1.2 * base)
    }

    @Test
    void delayClampsToTheCapForLargeRetryCounts() {
        long capped = EodBackoff.nextRetryDelayMs(20, 1_000L, 300_000L, RNG);
        assertThat(capped).isEqualTo(300_000L);
        long negative = EodBackoff.nextRetryDelayMs(-3, 1_000L, 300_000L, RNG);
        assertThat(negative).isBetween(800L, 1_199L); // clamped to retryCount 0
    }

    @Test
    void nextRetryAtIsNowPlusDelay() {
        long now = 1_752_000_000_000L;
        long at = EodBackoff.nextRetryAtMs(now, 1, 1_000L, 300_000L, RNG);
        assertThat(at).isGreaterThan(now);
        assertThat(at - now).isBetween(1_600L, 2_399L); // 2 * base ± 20%
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> EodBackoff.nextRetryDelayMs(0, 0L, 300_000L, RNG))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodBackoff.nextRetryDelayMs(0, 1_000L, 500L, RNG))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EodBackoff.nextRetryDelayMs(0, 1_000L, 300_000L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
