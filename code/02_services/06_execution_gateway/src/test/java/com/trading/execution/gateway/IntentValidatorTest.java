package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class IntentValidatorTest {
    private static IntentRecord valid() {
        return new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, "hash", null, "1", 0);
    }
    @Test void acceptsMatchingScopeAndRejectsExpiredOrWrongScope() {
        assertThat(IntentValidator.validate(valid(), "acct", "part", 100)).isEqualTo("accepted");
        assertThatThrownBy(() -> IntentValidator.validate(valid(), "other", "part", 100))
                .hasMessage("account scope mismatch");
        assertThatThrownBy(() -> IntentValidator.validate(valid(), "acct", "part", 10_001))
                .hasMessage("intent expired");
    }
    @Test void limitOrdersRequirePrice() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "LIMIT", null, "MIS", "DAY", "s", "1", "cfg", 1, null, "hash", null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessage("limit order requires positive limit_price_paise");
    }
}
