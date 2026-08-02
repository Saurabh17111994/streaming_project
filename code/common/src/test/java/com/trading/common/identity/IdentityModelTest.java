package com.trading.common.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.common.identity.IdentityModel.AccountScopeId;
import com.trading.common.identity.IdentityModel.CandidateId;
import com.trading.common.identity.IdentityModel.ExecutionAttemptId;
import com.trading.common.identity.IdentityModel.HaltRequestId;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import com.trading.common.identity.IdentityModel.PortfolioId;
import com.trading.common.identity.IdentityModel.PositionId;
import com.trading.common.identity.IdentityModel.PostbackEventId;
import com.trading.common.identity.IdentityModel.ReservationId;
import com.trading.common.identity.IdentityModel.TradeContextId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-043/R-074/R-075 — identity value semantics + validation.
 */
@DisplayName("R-043: identity classes have value semantics and validation")
class IdentityModelTest {

    @Test
    @DisplayName("previously non-equals identity classes now compare by value (R-043)")
    void valueSemanticsForAll() {
        // The 11 classes that previously lacked equals/hashCode.
        assertEquals(new CandidateId("a"), new CandidateId("a"));
        assertEquals(new ExecutionAttemptId("a"), new ExecutionAttemptId("a"));
        assertEquals(new TradeContextId("a"), new TradeContextId("a"));
        assertEquals(new PositionId("a"), new PositionId("a"));
        assertEquals(new PostbackEventId("a"), new PostbackEventId("a"));
        assertEquals(new AccountScopeId("a"), new AccountScopeId("a"));
        assertEquals(new PortfolioId("a"), new PortfolioId("a"));
        assertEquals(new ReservationId("a"), new ReservationId("a"));
        assertEquals(new HaltRequestId("a"), new HaltRequestId("a"));

        assertNotEquals(new CandidateId("a"), new CandidateId("b"));
        assertNotEquals(new CandidateId("a"), new ExecutionAttemptId("a"));

        assertEquals(new CandidateId("a").hashCode(), new CandidateId("a").hashCode());
    }

    @Test
    @DisplayName("null/blank identity values rejected at construction (R-074)")
    void rejectsNullBlank() {
        assertThrows(IllegalArgumentException.class, () -> new CandidateId(null));
        assertThrows(IllegalArgumentException.class, () -> new CandidateId("   "));
        assertThrows(IllegalArgumentException.class, () -> new AccountScopeId(null));
        assertThrows(IllegalArgumentException.class, () -> new InstrumentToken(0));
    }

    @Test
    @DisplayName("InstrumentToken range validation (R-075)")
    void instrumentTokenRange() {
        assertThrows(IllegalArgumentException.class, () -> new InstrumentToken(0));
        assertThrows(IllegalArgumentException.class, () -> new InstrumentToken(-5));
        assertEquals(26009, new InstrumentToken(26009).value());
    }
}
