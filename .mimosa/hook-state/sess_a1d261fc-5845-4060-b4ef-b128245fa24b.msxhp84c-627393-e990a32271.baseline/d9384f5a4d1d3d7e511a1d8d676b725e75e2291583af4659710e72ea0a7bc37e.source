package com.trading.common.schema.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * COMPAT-FLUSS-005: the raw-client composite-PK upsert matrix, re-verified on
 * every live run.
 *
 * <p>The matrix probe lives in main code ({@link CompositeKeyMatrixVerifier}) so
 * the SAME executable verification runs in-band during a DDL apply
 * ({@code com.trading.common.schema.ddl.DdlApplyTool}) — the apply gates on the
 * live matrix rather than referencing capability evidence. This test re-runs
 * that verifier and pins the documented 4-cell contract (evidence 2026-08-15,
 * {@code logs/schema-compat/composite-pk-raw-client-20260815.md}): configs 1/2/4
 * must FAIL with the {@link CompositeKeyMatrixVerifier#ICEBERG_ERROR} signature,
 * config 3 must PASS. If a future Fluss version changes any cell, this test
 * fails and the matrix + docs must be updated deliberately.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} (e.g. {@code localhost:9123}). Tagged
 * {@code integration} so a default {@code mvn test} does not require a cluster.
 * Scratch tables are prefixed {@code compat_cpk_} and dropped by the verifier.
 */
@Tag("integration")
class CompatFlussCompositeKeyIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(CompatFlussCompositeKeyIntegrationTest.class);

  private static final String PREFIX = "compat_cpk_";
  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  private static Connection connection;
  private static Admin admin;

  private static void connect() {
    String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
    assumeTrue(bootstrap != null && !bootstrap.isBlank(),
        "set FLUSS_BOOTSTRAP to run Fluss integration tests");
    if (connection != null) {
      return;
    }
    try {
      Configuration conf = new Configuration();
      conf.setString("bootstrap.servers", bootstrap);
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      LOG.info("compat-fluss-005: connected to {}", bootstrap);
    } catch (Exception e) {
      LOG.warn("compat-fluss-005: cannot connect to {} — {}", bootstrap, e.getMessage());
      assumeTrue(false, "Fluss cluster not available at " + bootstrap);
    }
  }

  @AfterAll
  static void cleanup() throws Exception {
    if (admin != null) {
      admin.close();
    }
    if (connection != null) {
      connection.close();
    }
  }

  // ── COMPAT-FLUSS-005: raw-client composite-PK matrix ─────────────────────

  /**
   * The four configuration cells of the verified matrix (see
   * {@link CompositeKeyMatrixVerifier#MATRIX} for the pinned configs).
   */
  @Test
  @DisplayName("COMPAT-FLUSS-005: raw-client composite-PK upsert matrix (kv.format-version × bucket key)")
  void compatFluss005CompositeKeyRawClientMatrix() throws Exception {
    connect();
    long stamp = System.nanoTime();
    CompositeKeyMatrixVerifier.Result result =
        CompositeKeyMatrixVerifier.verify(connection, admin, PREFIX + stamp, TIMEOUT);

    assertTrue(result.passed(),
        "matrix deviations:\n  " + String.join("\n  ", result.deviations()));
    assertEquals(4, result.cells().size(),
        "the documented matrix has exactly 4 cells");
    for (CompositeKeyMatrixVerifier.CellResult cell : result.cells()) {
      assertTrue(cell.matched(),
          "cell '" + cell.label() + "' expected "
              + (cell.expectedPass() ? "PASS" : "IcebergKeyEncoder failure")
              + " but got: " + cell.outcome());
    }

    LOG.info("compat-fluss-005: matrix re-verified — v1/v2×default fail with the "
        + "documented signature, v2+subset-bucket-key PASS (feature_candles_15s, "
        + "instruments use the working cell; Order_Lifecycle/Order_Correlation "
        + "stay Flink-connector-only); the same verifier gates DDL applies in-band");
  }
}
