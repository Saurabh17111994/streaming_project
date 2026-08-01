package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Full-suite compliance coverage at the unit level (the parts that need no live
 * Fluss cluster). Each assertion maps to a schema/storage test ID from
 * docs/08_implementation/11-testing-and-release.md:
 *
 * <ul>
 *   <li>SCHEMA-UNIT-002 — non-null routing identity (routing skew flagged)</li>
 *   <li>SCHEMA-UNIT-003 — unknown/placeholder schema version blocks executable authority</li>
 *   <li>COMPAT-FLUSS-004 — stale / conflict KV updates rejected (halt on conflict)</li>
 *   <li>immutable LOG — duplicate dropped, mutation violation</li>
 * </ul>
 */
class SchemaComplianceFullSuiteTest {

  @Test
  void immutableDuplicateIsDropped() {
    String h = ImmutabilityProtocol.canonicalHash("v1");
    assertThat(ImmutabilityProtocol.evaluate(h, h)).isEqualTo(ImmutabilityProtocol.Outcome.DUPLICATE);
  }

  @Test
  void immutableMutationIsViolation() {
    assertThat(ImmutabilityProtocol.evaluate("h1", "h2")).isEqualTo(ImmutabilityProtocol.Outcome.VIOLATION);
  }

  @Test
  void kvStaleAndConflictAreRejectedWithHalt() {
    // stale
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.evaluate(5, 3, false))).isTrue();
    // conflict (same version, different content)
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.evaluate(5, 5, false))).isTrue();
  }

  @Test
  void routingSkewIsFlagged() {
    SchemaManifestEntry e = new SchemaManifestEntry();
    e.tableName = "l";
    e.tableKind = "LOG";
    e.bucketKey = "";
    assertThat(RoutingKeyRule.check(List.of(e))).isNotEmpty();
  }

  @Test
  void placeholderVersionBlocksExecutableAuthority() {
    assertThat(SchemaState.PROPOSED.isExecutableAuthority()).isFalse();
    assertThat(SchemaState.REJECTED.isExecutableAuthority()).isFalse();
    assertThat(SchemaState.OBSERVED.isExecutableAuthority()).isFalse();
  }
}
