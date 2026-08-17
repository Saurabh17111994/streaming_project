package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for KV partial-update conflict / stale rules (COMPAT-FLUSS-004). */
class KvStateUpdateProtocolTest {

  @Test
  void newerVersionApplied() {
    assertThat(KvStateUpdateProtocol.evaluate(1, 2, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.APPLIED);
  }

  @Test
  void sameVersionSameContentIsDuplicate() {
    assertThat(KvStateUpdateProtocol.evaluate(2, 2, true))
        .isEqualTo(KvStateUpdateProtocol.Outcome.DUPLICATE);
  }

  @Test
  void sameVersionDifferentContentIsConflict() {
    assertThat(KvStateUpdateProtocol.evaluate(2, 2, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.CONFLICT);
  }

  @Test
  void olderVersionIsStale() {
    // When content matches, older version is STALE (idempotent re-delivery of old event)
    assertThat(KvStateUpdateProtocol.evaluate(2, 1, true))
        .isEqualTo(KvStateUpdateProtocol.Outcome.STALE);
    // When content differs, older version is REGRESSION (move backward unexpectedly)
    assertThat(KvStateUpdateProtocol.evaluate(2, 1, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.REGRESSION);
  }

  @Test
  void negativeExistingVersionIsUnknown() {
    assertThat(KvStateUpdateProtocol.evaluate(-1, 1, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.UNKNOWN);
  }

  @Test
  void conflictAndStaleRequireHalt() {
    // CONFLICT → halt (version collision with different content)
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.CONFLICT)).isTrue();
    // STALE → halt (older version rejected — must not overwrite newer state)
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.STALE)).isTrue();
    // REGRESSION → halt (value moved backward unexpectedly)
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.REGRESSION)).isTrue();
    // UNKNOWN → halt (ambiguous; quarantine + halt)
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.UNKNOWN)).isTrue();
    // APPLIED and DUPLICATE do not require halt
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.APPLIED)).isFalse();
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.DUPLICATE)).isFalse();
  }
}
