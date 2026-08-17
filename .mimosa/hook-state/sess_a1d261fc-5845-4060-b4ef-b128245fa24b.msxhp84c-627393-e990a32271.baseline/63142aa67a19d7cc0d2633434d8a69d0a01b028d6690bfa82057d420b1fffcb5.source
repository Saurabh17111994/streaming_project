package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for schema-state authority gating. */
class SchemaStateTest {

  @Test
  void onlyApprovedIsExecutableAuthority() {
    assertThat(SchemaState.APPROVED.isExecutableAuthority()).isTrue();
    assertThat(SchemaState.OBSERVED.isExecutableAuthority()).isFalse();
    assertThat(SchemaState.PROPOSED.isExecutableAuthority()).isFalse();
    assertThat(SchemaState.REJECTED.isExecutableAuthority()).isFalse();
  }
}
