package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PostbackCorrelatorTest {

    private final AttemptRef brokerRef = new AttemptRef("acc-1", "instr-1", "att-1", "tc-1");
    private final AttemptRef echoedRef = new AttemptRef("acc-1", "instr-2", "att-2", "tc-2");

    private InMemoryCorrelationIndex index() {
        return new InMemoryCorrelationIndex()
                .byBrokerOrderId("b-1", brokerRef)
                .byEchoedClientOrderRef("ref-b-2", echoedRef);
    }

    @Test
    void brokerOrderIdTakesPrecedence() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, 0L);
        PostbackCorrelator.CorrelationResult r = PostbackCorrelator.correlate(p, index());
        assertThat(r.outcome()).isEqualTo(PostbackCorrelator.Outcome.CORRELATED);
        assertThat(((PostbackCorrelator.Correlated) r).ref()).isEqualTo(brokerRef);
    }

    @Test
    void echoedClientRefUsedWhenNoBrokerId() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-2", "BUY", 10, 0, 10, 1000L, 0L);
        // override brokerOrderId to empty to force the echoed-ref fallback
        NormalizedPostback noBroker = new NormalizedPostback(
                p.postbackEventId(), p.sourceEventId(), p.sourceSequence(),
                p.fingerprint() + "-nobroker", p.fingerprintVersion(), null,
                "ref-b-2", p.accountScopeId(), p.instrumentToken(), p.exchange(),
                p.symbol(), p.side(), p.orderStatus(), p.cumulativeQty(), p.pendingQty(),
                p.fillQty(), p.fillPricePaise(), p.eventTimeMs(), p.receiveTimeMs(),
                p.mappingVersion(), p.originalPayloadHash(), p.tradeContextId());
        PostbackCorrelator.CorrelationResult r = PostbackCorrelator.correlate(noBroker, index());
        assertThat(r.outcome()).isEqualTo(PostbackCorrelator.Outcome.CORRELATED);
        assertThat(((PostbackCorrelator.Correlated) r).ref()).isEqualTo(echoedRef);
    }

    @Test
    void missingBothIdsQuarantines() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, 0L);
        NormalizedPostback neither = new NormalizedPostback(
                p.postbackEventId(), p.sourceEventId(), p.sourceSequence(), "fp-x",
                p.fingerprintVersion(), null, null, p.accountScopeId(), p.instrumentToken(),
                p.exchange(), p.symbol(), p.side(), p.orderStatus(), p.cumulativeQty(),
                p.pendingQty(), p.fillQty(), p.fillPricePaise(), p.eventTimeMs(),
                p.receiveTimeMs(), p.mappingVersion(), p.originalPayloadHash(), p.tradeContextId());
        PostbackCorrelator.CorrelationResult r = PostbackCorrelator.correlate(neither, index());
        assertThat(r.outcome()).isEqualTo(PostbackCorrelator.Outcome.QUARANTINED);
        assertThat(((PostbackCorrelator.Quarantined) r).reason())
                .isEqualTo(QuarantineReason.MISSING_BROKER_ID);
    }

    @Test
    void unknownBrokerFallsBackToReconciliation() {
        InMemoryCorrelationIndex idx = index()
                .byReconciliation("acc-1", "b-9", brokerRef);
        NormalizedPostback p = TestPostbacks.fill(1L, "b-9", "BUY", 10, 0, 10, 1000L, 0L);
        PostbackCorrelator.CorrelationResult r = PostbackCorrelator.correlate(p, idx);
        assertThat(r.outcome()).isEqualTo(PostbackCorrelator.Outcome.CORRELATED);
        assertThat(((PostbackCorrelator.Correlated) r).ref()).isEqualTo(brokerRef);
    }

    @Test
    void unresolvableBrokerQuarantines() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-99", "BUY", 10, 0, 10, 1000L, 0L);
        PostbackCorrelator.CorrelationResult r = PostbackCorrelator.correlate(p, index());
        assertThat(r.outcome()).isEqualTo(PostbackCorrelator.Outcome.QUARANTINED);
        assertThat(((PostbackCorrelator.Quarantined) r).reason())
                .isEqualTo(QuarantineReason.NO_MATCHING_INSTRUCTION);
    }

    @Test
    void brokerAndEchoedContradictQuarantinesAmbiguous() {
        InMemoryCorrelationIndex idx = index()
                .byEchoedClientOrderRef("ref-b-1", echoedRef); // contradicts brokerRef
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, 0L);
        PostbackCorrelator.CorrelationResult r = PostbackCorrelator.correlate(p, idx);
        assertThat(r.outcome()).isEqualTo(PostbackCorrelator.Outcome.QUARANTINED);
        assertThat(((PostbackCorrelator.Quarantined) r).reason())
                .isEqualTo(QuarantineReason.AMBIGUOUS_CORRELATION);
    }
}
