package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.ImmutabilityProtocol;
import org.junit.jupiter.api.Test;

class ExecutionIntentFeedProtocolTest {

    @Test
    void firstWriteIsAccepted() {
        var result = ExecutionIntentFeedProtocol.verify("ei-1", null, "hash-a", 10L);
        assertEquals(ImmutabilityProtocol.Outcome.ACCEPTED, result.outcome());
        assertTrue(result.accepted());
        assertFalse(result.duplicate());
        assertFalse(result.violation());
    }

    @Test
    void sameHashIsIdempotentDuplicate() {
        var result = ExecutionIntentFeedProtocol.verify("ei-1", "hash-a", "hash-a", 10L);
        assertEquals(ImmutabilityProtocol.Outcome.DUPLICATE, result.outcome());
        assertFalse(ExecutionIntentFeedProtocol.requiresHalt(result));
    }

    @Test
    void changedHashIsViolationAndProducesEnforcementEvent() {
        var result = ExecutionIntentFeedProtocol.verify("ei-1", "hash-a", "hash-b", 10L);
        assertEquals(ImmutabilityProtocol.Outcome.VIOLATION, result.outcome());
        assertTrue(ExecutionIntentFeedProtocol.requiresHalt(result));
        assertEquals("hash-b",
                ExecutionIntentFeedProtocol.enforcementEvent(result).requestHash());
    }

    @Test
    void cleanOutcomesCannotProduceEnforcementEvent() {
        var accepted = ExecutionIntentFeedProtocol.verify("ei-1", null, "hash-a", 10L);
        assertThrows(IllegalStateException.class,
                () -> ExecutionIntentFeedProtocol.enforcementEvent(accepted));
    }
}
