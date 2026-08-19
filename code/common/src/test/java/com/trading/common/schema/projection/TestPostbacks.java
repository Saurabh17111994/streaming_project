package com.trading.common.schema.projection;

/** Builder for valid {@link NormalizedPostback} fixtures with correct fingerprints. */
public final class TestPostbacks {

    private TestPostbacks() {}

    public static final String ACCOUNT = "acc-1";
    public static final String INSTR = "wti";
    public static final long TOKEN = 1001L;
    public static final String MAP = "1";

    public static NormalizedPostback fill(long seq, String brokerOrderId, String side,
            long cumulative, long pending, long fillQty, long pricePaise, long nowMs) {
        String peid = "pb-" + brokerOrderId + "-" + seq;
        String sourceEventId = "evt-" + brokerOrderId + "-" + seq;
        String echoed = "ref-" + brokerOrderId;
        String fp = PostbackFingerprint.compute(MAP, PostbackFingerprint.canonicalFrom(
                MAP, peid, sourceEventId, seq, brokerOrderId, echoed, ACCOUNT, side,
                "PARTIAL", cumulative, pending, fillQty, pricePaise, nowMs, nowMs));
        return new NormalizedPostback(
                peid, sourceEventId, seq, fp, MAP, brokerOrderId, echoed, ACCOUNT, TOKEN,
                "CME", INSTR, side, "PARTIAL", cumulative, pending, fillQty, pricePaise,
                nowMs, nowMs, MAP, "hash-" + seq, "tc-" + brokerOrderId);
    }

    public static NormalizedPostback status(long seq, String brokerOrderId, String status,
            long cumulative, long pending, long nowMs) {
        String peid = "pb-" + brokerOrderId + "-s" + seq;
        String sourceEventId = "evt-" + brokerOrderId + "-s" + seq;
        String echoed = "ref-" + brokerOrderId;
        String fp = PostbackFingerprint.compute(MAP, PostbackFingerprint.canonicalFrom(
                MAP, peid, sourceEventId, seq, brokerOrderId, echoed, ACCOUNT, null,
                status, cumulative, pending, 0, 0, nowMs, nowMs));
        return new NormalizedPostback(
                peid, sourceEventId, seq, fp, MAP, brokerOrderId, echoed, ACCOUNT, TOKEN,
                "CME", INSTR, null, status, cumulative, pending, 0, 0,
                nowMs, nowMs, MAP, "hash-" + seq, "tc-" + brokerOrderId);
    }

    public static NormalizedPostback withFingerprintMismatch(NormalizedPostback p) {
        return new NormalizedPostback(
                p.postbackEventId(), p.sourceEventId(), p.sourceSequence(), "fp-bad",
                p.fingerprintVersion(), p.brokerOrderId(), p.echoedClientOrderRef(),
                p.accountScopeId(), p.instrumentToken(), p.exchange(), p.symbol(), p.side(),
                p.orderStatus(), p.cumulativeQty(), p.pendingQty(), p.fillQty(),
                p.fillPricePaise(), p.eventTimeMs(), p.receiveTimeMs(), p.mappingVersion(),
                p.originalPayloadHash(), p.tradeContextId());
    }
}
