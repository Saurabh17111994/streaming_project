package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class IntentDeduplicatorTest {
    @Test void deferredDeliveryCanBeRetriedBeforeCommit() {
        IntentDeduplicator d = new IntentDeduplicator();
        assertThat(d.classify("i", "h")).isEqualTo(IntentDeduplicator.Outcome.FIRST);
        assertThat(d.classify("i", "h")).isEqualTo(IntentDeduplicator.Outcome.FIRST);
        d.commit("i", "h");
        assertThat(d.classify("i", "h")).isEqualTo(IntentDeduplicator.Outcome.DUPLICATE);
    }
    @Test void changedContentIsAProtocolViolation() {
        IntentDeduplicator d = new IntentDeduplicator(); d.commit("i", "h1");
        assertThat(d.classify("i", "h2")).isEqualTo(IntentDeduplicator.Outcome.HASH_VIOLATION);
    }
}
