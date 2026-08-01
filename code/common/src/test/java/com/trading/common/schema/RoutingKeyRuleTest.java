package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the routing-identity rule (LOG tables route by `bucket.key`). */
class RoutingKeyRuleTest {

  private SchemaManifestEntry entry(String kind, String bucketKey, String pk) {
    SchemaManifestEntry e = new SchemaManifestEntry();
    e.tableName = "t";
    e.tableKind = kind;
    e.bucketKey = bucketKey;
    e.primaryKey = pk;
    return e;
  }

  @Test
  void logTableWithoutBucketKeyIsViolation() {
    var v = RoutingKeyRule.check(List.of(entry("LOG", "  ", null)));
    assertThat(v).hasSize(1);
    assertThat(v.get(0).tableName).isEqualTo("t");
    assertThat(v.get(0).reason).contains("bucket.key");
  }

  @Test
  void logTableWithBucketKeyIsClean() {
    assertThat(RoutingKeyRule.check(List.of(entry("LOG", "instrument_id", null)))).isEmpty();
  }

  @Test
  void kvTableNeedsNoBucketKey() {
    // KV (primary-key) tables route by primary key, not bucket.key.
    assertThat(RoutingKeyRule.check(List.of(entry("KV", null, "id")))).isEmpty();
  }
}
