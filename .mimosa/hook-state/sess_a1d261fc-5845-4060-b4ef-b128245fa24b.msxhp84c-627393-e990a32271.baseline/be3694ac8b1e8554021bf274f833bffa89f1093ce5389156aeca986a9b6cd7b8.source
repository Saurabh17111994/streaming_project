package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for EOD offload / source-expiry gating. */
class EodControllerStateTest {

  @Test
  void onlyVerifiedPermitsSourceExpiry() {
    assertThat(EodControllerState.VERIFIED.permitsSourceExpiry()).isTrue();
    assertThat(EodControllerState.FAILED_RETRYABLE.permitsSourceExpiry()).isFalse();
    assertThat(EodControllerState.FAILED_MANUAL.permitsSourceExpiry()).isFalse();
    assertThat(EodControllerState.PENDING.permitsSourceExpiry()).isFalse();
    assertThat(EodControllerState.WRITING.permitsSourceExpiry()).isFalse();
    assertThat(EodControllerState.COMMITTED.permitsSourceExpiry()).isFalse();
    assertThat(EodControllerState.VERIFYING.permitsSourceExpiry()).isFalse();
  }

  @Test
  void allNonVerifiedStatesRequireRetentionExtension() {
    for (EodControllerState s : EodControllerState.values()) {
      if (s == EodControllerState.VERIFIED) {
        assertThat(s.requiresRetentionExtension()).isFalse();
      } else {
        assertThat(s.requiresRetentionExtension())
            .as(s + " requires retention extension")
            .isTrue();
      }
    }
  }

  @Test
  void onlyFailedRetryableIsRetryable() {
    assertThat(EodControllerState.FAILED_RETRYABLE.isRetryable()).isTrue();
    assertThat(EodControllerState.FAILED_MANUAL.isRetryable()).isFalse();
    assertThat(EodControllerState.PENDING.isRetryable()).isFalse();
    assertThat(EodControllerState.VERIFIED.isRetryable()).isFalse();
  }
}
