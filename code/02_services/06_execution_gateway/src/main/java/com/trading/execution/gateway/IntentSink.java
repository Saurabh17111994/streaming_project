package com.trading.execution.gateway;

/** Result-aware handoff so HALTED intents remain replayable rather than being acknowledged. */
@FunctionalInterface
public interface IntentSink {
    enum Result { FORWARDED, DEFERRED, REJECTED }
    Result forward(IntentRecord intent) throws Exception;
}
