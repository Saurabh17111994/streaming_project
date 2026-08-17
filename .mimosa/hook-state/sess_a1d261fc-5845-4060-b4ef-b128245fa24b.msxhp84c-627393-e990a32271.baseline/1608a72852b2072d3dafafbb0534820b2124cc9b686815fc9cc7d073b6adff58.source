package com.trading.ingestion.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-004: ValidityClassification enum — all states defined.
 */
@DisplayName("ING-UNIT-004: ValidityClassification")
class ValidityClassificationTest {

    @Test
    @DisplayName("All classification states exist (7 total: 4 core + 3 extended)")
    void allStatesDefined() {
        ValidityClassification[] states = ValidityClassification.values();
        assertEquals(7, states.length, "Must have exactly 7 validity states");
    }

    @Test
    @DisplayName("VALID_TRADE present")
    void validTrade() {
        assertEquals("VALID_TRADE", ValidityClassification.VALID_TRADE.name());
    }

    @Test
    @DisplayName("VALID_NON_TRADE present")
    void validNonTrade() {
        assertEquals("VALID_NON_TRADE", ValidityClassification.VALID_NON_TRADE.name());
    }

    @Test
    @DisplayName("INVALID_VALUES present")
    void invalidValues() {
        assertEquals("INVALID_VALUES", ValidityClassification.INVALID_VALUES.name());
    }

    @Test
    @DisplayName("MISSING_INSTRUMENT present")
    void missingInstrument() {
        assertEquals("MISSING_INSTRUMENT", ValidityClassification.MISSING_INSTRUMENT.name());
    }
}
