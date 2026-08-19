package com.trading.common.schema.projection;

import java.util.Objects;
import java.util.Optional;

/**
 * Correlation precedence for a {@link NormalizedPostback} (T6, CHG-045):
 * {@code broker_order_id} &rarr; echoed {@code client_order_ref} &rarr;
 * approved reconciliation result. Missing, multiple, or conflicting matches go
 * to Postback_Quarantine, emit audit evidence, and the driver halts the scope
 * (05-execution-core.md correlation contract; 16_postback_quarantine.sql).
 */
public final class PostbackCorrelator {

    /** Outcome of one correlation attempt. */
    public enum Outcome { CORRELATED, QUARANTINED }

    public sealed interface CorrelationResult {
        Outcome outcome();
        AttemptRef ref();
        QuarantineReason reason();
        String detail();
    }

    public record Correlated(AttemptRef ref) implements CorrelationResult {
        @Override public Outcome outcome() { return Outcome.CORRELATED; }
        @Override public QuarantineReason reason() { return null; }
        @Override public String detail() { return null; }
    }

    public record Quarantined(QuarantineReason reason, String detail, AttemptRef ref)
            implements CorrelationResult {
        @Override public Outcome outcome() { return Outcome.QUARANTINED; }
    }

    private PostbackCorrelator() {}

    public static CorrelationResult correlate(NormalizedPostback p, CorrelationIndex index) {
        Objects.requireNonNull(p, "postback");
        Objects.requireNonNull(index, "index");

        if (!p.hasBrokerOrderId()) {
            // No broker id: fall back to the echoed client order ref.
            if (p.echoedClientOrderRef() == null || p.echoedClientOrderRef().isBlank()) {
                return new Quarantined(QuarantineReason.MISSING_BROKER_ID,
                        "neither broker_order_id nor echoed client_order_ref present", null);
            }
            Optional<AttemptRef> byRef = index.byEchoedClientOrderRef(p.echoedClientOrderRef());
            if (byRef.isEmpty()) {
                return new Quarantined(QuarantineReason.NO_MATCHING_INSTRUCTION,
                        "echoed client_order_ref " + p.echoedClientOrderRef() + " has no match", null);
            }
            return new Correlated(byRef.get());
        }

        Optional<AttemptRef> byBroker = index.byBrokerOrderId(p.brokerOrderId());
        if (byBroker.isEmpty()) {
            // Broker id present but not matched: reconciliation fallback.
            Optional<AttemptRef> reconciled =
                    index.approvedReconciliation(p.accountScopeId(), p.brokerOrderId());
            if (reconciled.isEmpty()) {
                return new Quarantined(QuarantineReason.NO_MATCHING_INSTRUCTION,
                        "broker_order_id " + p.brokerOrderId() + " has no match and no approved "
                        + "reconciliation", null);
            }
            return new Correlated(reconciled.get());
        }

        AttemptRef ref = byBroker.get();
        // Broker id resolved. Sanity-check it does not contradict an echoed ref.
        if (p.echoedClientOrderRef() != null && !p.echoedClientOrderRef().isBlank()) {
            Optional<AttemptRef> byRef = index.byEchoedClientOrderRef(p.echoedClientOrderRef());
            if (byRef.isPresent() && !byRef.get().equals(ref)) {
                return new Quarantined(QuarantineReason.AMBIGUOUS_CORRELATION,
                        "broker_order_id and echoed client_order_ref resolve to different attempts",
                        ref);
            }
        }
        return new Correlated(ref);
    }
}
