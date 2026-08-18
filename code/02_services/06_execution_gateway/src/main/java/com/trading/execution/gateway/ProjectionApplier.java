package com.trading.execution.gateway;

import java.time.Clock;

/** Resumable coordinator for independent Fluss projection writes. */
public final class ProjectionApplier {
    private final ProjectionWriter writer;
    private final ProjectionLedgerStore ledger;
    private final Clock clock;

    public ProjectionApplier(ProjectionWriter writer, ProjectionLedgerStore ledger) {
        this(writer, ledger, Clock.systemUTC());
    }
    ProjectionApplier(ProjectionWriter writer, ProjectionLedgerStore ledger, Clock clock) {
        this.writer = writer; this.ledger = ledger; this.clock = clock;
    }

    public synchronized boolean apply(NormalizedExecutionEvent event) throws Exception {
        ProjectionLedgerStore.Entry current = ledger.lookup(event.postbackEventId());
        if (current != null && ProjectionLedger.terminal(current.state())) return false;
        ProjectionLedger.State state = current == null ? ProjectionLedger.State.RECEIVED : current.state();
        if (current == null) ledger.put(entry(event, state, null, null));
        try {
            if (state == ProjectionLedger.State.RECEIVED) {
                writer.writeAudit(event);
                state = ProjectionLedger.advance(state, ProjectionLedger.State.AUDIT_WRITTEN);
                ledger.put(entry(event, state, "RECEIVED", null));
            }
            if (state == ProjectionLedger.State.AUDIT_WRITTEN) {
                writer.writeLifecycle(event);
                state = ProjectionLedger.advance(state, ProjectionLedger.State.LIFECYCLE_APPLIED);
                ledger.put(entry(event, state, "AUDIT_WRITTEN", null));
            }
            if (state == ProjectionLedger.State.LIFECYCLE_APPLIED) {
                writer.writePosition(event);
                state = ProjectionLedger.advance(state, ProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED);
                ledger.put(entry(event, state, "LIFECYCLE_APPLIED", null));
            }
            state = ProjectionLedger.advance(state, ProjectionLedger.State.COMPLETE);
            ledger.put(entry(event, state, "POSITION_APPLIED_OR_NOT_REQUIRED", clock.millis()));
            return true;
        } catch (Exception failure) {
            ledger.put(entry(event, state, currentStateName(state), null, failure.getMessage()));
            throw failure;
        }
    }

    private ProjectionLedgerStore.Entry entry(NormalizedExecutionEvent e, ProjectionLedger.State state,
            String prior, Long completed) {
        return entry(e, state, prior, completed, null);
    }
    private ProjectionLedgerStore.Entry entry(NormalizedExecutionEvent e, ProjectionLedger.State state,
            String prior, Long completed, String error) {
        return new ProjectionLedgerStore.Entry(e.postbackEventId(), state, prior, 0,
                error, state == ProjectionLedger.State.COMPLETE ? "COMPLETE" : "OPEN",
                clock.millis(), completed);
    }
    private static String currentStateName(ProjectionLedger.State state) { return state.name(); }
}
