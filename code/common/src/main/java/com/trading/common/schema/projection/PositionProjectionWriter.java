package com.trading.common.schema.projection;

import com.trading.common.model.PositionState;
import com.trading.common.schema.KvStateUpdateProtocol;
import com.trading.common.schema.position.PositionLifecycle;
import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsColumns;
import java.util.Objects;

/**
 * Serializer half of the position projection (T6, CHG-045): maps an
 * authoritative {@link NautilusPositionEvent} into a {@link PositionSnapshot}
 * (Positions KV row) and applies the version gate. It performs NO arithmetic —
 * open/closed/avg come unchanged from Nautilus; it only validates invariants
 * and rejects stale/conflict/inconsistent updates. A Nautilus event that
 * violates the quantity/state invariants is {@code POSITION_VIOLATION}
 * (quarantine + halt).
 */
public final class PositionProjectionWriter {

    private PositionProjectionWriter() {}

    public enum Outcome { APPLIED, DUPLICATE, STALE, VIOLATION }

    public record PositionWriteResult(Outcome outcome, PositionSnapshot snapshot,
            QuarantineReason reason, String detail) {

        public static PositionWriteResult applied(PositionSnapshot s) {
            return new PositionWriteResult(Outcome.APPLIED, s, null, null);
        }
        public static PositionWriteResult duplicate(PositionSnapshot s) {
            return new PositionWriteResult(Outcome.DUPLICATE, s, null, null);
        }
        public static PositionWriteResult stale(PositionSnapshot s) {
            return new PositionWriteResult(Outcome.STALE, s, QuarantineReason.STALE_EVENT,
                    "stale position version " + s.sourceVersion());
        }
        public static PositionWriteResult violation(QuarantineReason r, String detail) {
            return new PositionWriteResult(Outcome.VIOLATION, null, r, detail);
        }
    }

    /**
     * Serialize the Nautilus event into a Positions row under the version gate.
     *
     * @param current current row (null = none)
     * @param event   the Nautilus-computed position event (authoritative)
     * @param nowMs   deterministic timestamp
     */
    public static PositionWriteResult apply(PositionSnapshot current,
            NautilusPositionEvent event, long nowMs) {
        Objects.requireNonNull(event, "event");

        // Invariant validation (no arithmetic) before the version gate.
        if (!eventInvariantsHold(event)) {
            return PositionWriteResult.violation(QuarantineReason.POSITION_VIOLATION,
                    "Nautilus event violates quantity/state invariants");
        }

        boolean contentMatches = current != null
                && current.sourceEventId().equals(event.sourceEventId());
        long currentVersion = current == null ? 0L : current.sourceVersion();
        switch (KvStateUpdateProtocol.evaluate(currentVersion, event.sourceSequence(),
                contentMatches)) {
            case DUPLICATE -> { return PositionWriteResult.duplicate(current); }
            case STALE, REGRESSION -> { return PositionWriteResult.stale(current); }
            case CONFLICT, UNKNOWN -> {
                return PositionWriteResult.violation(QuarantineReason.POSITION_VIOLATION,
                        "version check for position " + event.positionId());
            }
            case APPLIED -> { /* fall through */ }
        }

        PositionSnapshot next = new PositionSnapshot(
                event.positionId(),
                event.tradeContextId(),
                event.accountScopeId(),
                event.instrumentToken(),
                event.exchange(),
                event.symbol(),
                event.side(),
                event.state(),
                event.openQuantity(),
                event.closedQuantity(),
                event.averageEntryPaise(),
                event.averageExitPaise(),
                event.sourceEventId(),
                event.sourceSequence(),
                current == null ? nowMs : current.createdTs(),
                nowMs,
                PositionsColumns.SCHEMA_VERSION_V2);
        return PositionWriteResult.applied(next);
    }

    /**
     * Validate the Nautilus-computed event without recomputing it: state is
     * consistent with the derived quantity reading, and quantities are sane.
     */
    static boolean eventInvariantsHold(NautilusPositionEvent e) {
        PositionState derived =
                PositionLifecycle.derive(e.openQuantity(), e.closedQuantity(), true);
        if (derived == PositionState.UNKNOWN) {
            return false;
        }
        if (e.state() == PositionState.FLAT) {
            return e.openQuantity() == 0 && e.closedQuantity() == 0;
        }
        // The declared state must match the quantity-derived reading — the
        // projection never recomputes, but it refuses an internally-contradictory
        // Nautilus event (e.g. open==closed yet OPEN).
        return e.state() == derived;
    }
}
