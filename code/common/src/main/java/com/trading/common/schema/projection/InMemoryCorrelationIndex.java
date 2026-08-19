package com.trading.common.schema.projection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Pure-JVM {@link CorrelationIndex} backed by hash maps (offline tests/drills). */
public final class InMemoryCorrelationIndex implements CorrelationIndex {

    private final Map<String, List<AttemptRef>> byBroker = new HashMap<>();
    private final Map<String, List<AttemptRef>> byRef = new HashMap<>();
    private final Map<String, AttemptRef> byReconciliation = new HashMap<>();

    public InMemoryCorrelationIndex byBrokerOrderId(String brokerOrderId, AttemptRef ref) {
        byBroker.computeIfAbsent(brokerOrderId, k -> new ArrayList<>()).add(ref);
        return this;
    }

    public InMemoryCorrelationIndex byEchoedClientOrderRef(String clientOrderRef, AttemptRef ref) {
        byRef.computeIfAbsent(clientOrderRef, k -> new ArrayList<>()).add(ref);
        return this;
    }

    public InMemoryCorrelationIndex byReconciliation(String accountScopeId, String brokerOrderId,
            AttemptRef ref) {
        byReconciliation.put(key(accountScopeId, brokerOrderId), ref);
        return this;
    }

    private static String key(String a, String b) {
        return (a == null ? "" : a) + "\u0000" + (b == null ? "" : b);
    }

    private static Optional<AttemptRef> single(List<AttemptRef> list) {
        return list == null || list.size() != 1 ? Optional.empty() : Optional.of(list.get(0));
    }

    @Override
    public Optional<AttemptRef> byBrokerOrderId(String brokerOrderId) {
        return single(byBroker.get(brokerOrderId));
    }

    @Override
    public Optional<AttemptRef> byEchoedClientOrderRef(String clientOrderRef) {
        return single(byRef.get(clientOrderRef));
    }

    @Override
    public Optional<AttemptRef> approvedReconciliation(String accountScopeId, String brokerOrderId) {
        return Optional.ofNullable(byReconciliation.get(key(accountScopeId, brokerOrderId)));
    }

    /** Test/audit: how many distinct entries are registered. */
    public int size(Function<Map<String, ?>, Integer> count) {
        return count.apply(byBroker);
    }
}
