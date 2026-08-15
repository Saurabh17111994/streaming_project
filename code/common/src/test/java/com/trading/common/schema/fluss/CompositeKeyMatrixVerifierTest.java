package com.trading.common.schema.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure-JVM pin for {@link CompositeKeyMatrixVerifier}: the documented 4-cell
 * matrix shape (no cluster needed) and the expected-outcome matching used by
 * both the env-gated integration test and the in-band DDL apply gate. If the
 * pinned cell configs drift, this test fails and the matrix + docs must be
 * updated deliberately (COMPAT-FLUSS-005).
 */
class CompositeKeyMatrixVerifierTest {

  @Test
  void matrixPinsTheDocumentedFourCells() {
    assertEquals(4, CompositeKeyMatrixVerifier.MATRIX.size());
    CompositeKeyMatrixVerifier.CellSpec c1 = CompositeKeyMatrixVerifier.MATRIX.get(0);
    CompositeKeyMatrixVerifier.CellSpec c2 = CompositeKeyMatrixVerifier.MATRIX.get(1);
    CompositeKeyMatrixVerifier.CellSpec c3 = CompositeKeyMatrixVerifier.MATRIX.get(2);
    CompositeKeyMatrixVerifier.CellSpec c4 = CompositeKeyMatrixVerifier.MATRIX.get(3);

    // Cells 1/2: default (full-PK) bucket key — documented failure.
    for (CompositeKeyMatrixVerifier.CellSpec spec : List.of(c1, c2)) {
      assertFalse(spec.expectedPass(), spec.label() + " must expect failure");
      assertEquals(List.of("k1", "k2"), spec.bucketKeys(), spec.label() + " bucket key");
    }
    // Cells 3/4: single-field subset bucket key — cell 3 is the working cell.
    for (CompositeKeyMatrixVerifier.CellSpec spec : List.of(c3, c4)) {
      assertEquals(List.of("k1"), spec.bucketKeys(), spec.label() + " bucket key");
    }
    assertTrue(c3.expectedPass(), "v2 + subset bucket key must expect PASS");
    assertFalse(c4.expectedPass(), "v1 + subset bucket key must expect failure");
    assertEquals("2", c3.kvFormatVersion());

    // kv format versions: v1 absent / explicit, v2 explicit.
    assertEquals(null, c1.kvFormatVersion());
    assertEquals("2", c2.kvFormatVersion());
    assertEquals("1", c4.kvFormatVersion());
  }

  @Test
  void expectedPassMatchedOnlyByPass() {
    CompositeKeyMatrixVerifier.CellSpec pass =
        new CompositeKeyMatrixVerifier.CellSpec("x", List.of("k1"), "2", true);
    assertTrue(pass.matches("PASS"));
    assertFalse(pass.matches(CompositeKeyMatrixVerifier.ICEBERG_ERROR));
    assertFalse(pass.matches("some other error"));
    assertFalse(pass.matches(null));
  }

  @Test
  void expectedFailMatchedOnlyByIcebergSignature() {
    CompositeKeyMatrixVerifier.CellSpec fail =
        new CompositeKeyMatrixVerifier.CellSpec("y", List.of("k1", "k2"), null, false);
    assertTrue(fail.matches(CompositeKeyMatrixVerifier.ICEBERG_ERROR));
    assertTrue(fail.matches(CompositeKeyMatrixVerifier.ICEBERG_ERROR + "\nstack"));
    assertFalse(fail.matches("PASS"));
    assertFalse(fail.matches("connection refused"));
    assertFalse(fail.matches(null));
  }

  @Test
  void deviationsDescribeTheDriftedCell() {
    CompositeKeyMatrixVerifier.CellSpec pass =
        new CompositeKeyMatrixVerifier.CellSpec("working cell", List.of("k1"), "2", true);
    assertFalse(pass.matches(CompositeKeyMatrixVerifier.ICEBERG_ERROR),
        "a working cell that starts failing is a deliberate matrix change");
  }
}
