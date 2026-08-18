package com.trading.execution.gateway;

import java.util.concurrent.atomic.AtomicReference;

/** Separate health/readiness dimensions; health never implies execution readiness. */
public final class GatewayReadiness {
    public record Snapshot(boolean healthy, boolean flussReady, boolean protocolReady,
                           boolean durableWriteReady, String reason) {
        public boolean executionReady() {
            return healthy && flussReady && protocolReady && durableWriteReady;
        }
    }

    private final AtomicReference<Snapshot> state = new AtomicReference<>(
            new Snapshot(true, false, false, false, "starting"));

    public Snapshot snapshot() { return state.get(); }
    public void fluss(boolean ready, String reason) { update(s -> new Snapshot(s.healthy(), ready,
            s.protocolReady(), s.durableWriteReady(), reason)); }
    public void protocol(boolean ready, String reason) { update(s -> new Snapshot(s.healthy(),
            s.flussReady(), ready, s.durableWriteReady(), reason)); }
    public void durableWrites(boolean ready, String reason) { update(s -> new Snapshot(s.healthy(),
            s.flussReady(), s.protocolReady(), ready, reason)); }
    public void fail(String reason) { state.set(new Snapshot(false, false, false, false, reason)); }

    private void update(java.util.function.UnaryOperator<Snapshot> fn) {
        state.updateAndGet(fn);
    }
}
