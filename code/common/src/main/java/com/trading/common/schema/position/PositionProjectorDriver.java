package com.trading.common.schema.position;

import com.trading.common.model.PositionState;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.fluss.row.GenericRow;

/**
 * The SCH-20 operator-wiring core (Action Capture, Phase 4): a stateful driver
 * that holds the current {@link PositionSnapshot} per {@code position_id} in
 * memory, mints {@code position_id}s, and projects fills through the pure
 * {@link PositionProjector} with version gating.
 *
 * <p>Position identity (05-execution-core.md &rarr; "Position states"): account/instrument/side
 * uniqueness; {@code position_id} is minted
 * on the first uniquely correlated fill that creates exposure; a re-entry after
 * a full close (CLOSED) mints a NEW position_id. Minted ids are deterministic
 * ({@code pos-<account>-<instrumentToken>-<side>-<cycle>}) so a clean replay
 * converges.
 *
 * <p>Two feed paths:
 * <ul>
 *   <li>{@link #feed(GenericRow, FillContext, long)} — the operator path:
 *       resolves/mints the position id, maps the Fills row via
 *       {@link FillEventMapper}, and projects;</li>
 *   <li>{@link #feed(FillEvent, long)} — callers that already hold a resolved
 *       {@link FillEvent} (e.g. a Fluss changelog consumer reading pre-mapped
 *       rows).</li>
 * </ul>
 *
 * <p>Rejections (STALE / VIOLATION) are reported, never swallowed — the caller
 * (Action Capture) owns quarantine + halt. Deterministic: {@code nowMs} is a
 * parameter, never {@code System.currentTimeMillis()}.
 */
public final class PositionProjectorDriver {

    /** Account/instrument/side uniqueness key (dossier position protocol). */
    public record PositionKey(String accountScopeId, long instrumentToken, String side) {}

    public enum FeedOutcome {
        /** Projected — the new snapshot replaced the previous one. */
        APPLIED,
        /** Same source version + event already reflected — no-op. */
        DUPLICATE,
        /** Older source version — rejected. */
        STALE,
        /** Quantity/lifecycle violation — rejected, requires quarantine + halt. */
        VIOLATION,
        /** The row is not a fill (fill_qty missing/non-positive) — ignored. */
        NOT_A_FILL
    }

    public record FeedResult(FeedOutcome outcome, PositionSnapshot snapshot,
                             String positionId, String reason) {

        public static FeedResult applied(PositionSnapshot s, String positionId) {
            return new FeedResult(FeedOutcome.APPLIED, s, positionId, null);
        }

        public static FeedResult duplicate(PositionSnapshot s, String positionId) {
            return new FeedResult(FeedOutcome.DUPLICATE, s, positionId, null);
        }

        public static FeedResult stale(PositionSnapshot current, String positionId) {
            return new FeedResult(FeedOutcome.STALE, current, positionId,
                    "stale fill version " + current.sourceVersion());
        }

        public static FeedResult violation(String positionId, String reason) {
            return new FeedResult(FeedOutcome.VIOLATION, null, positionId, reason);
        }

        public static FeedResult notAFill(String positionId) {
            return new FeedResult(FeedOutcome.NOT_A_FILL, null, positionId,
                    "row is not a fill (fill_qty missing/non-positive)");
        }
    }

    private final Map<PositionKey, String> active = new HashMap<>();
    private final Map<PositionKey, Integer> cycles = new HashMap<>();
    private final Map<String, PositionSnapshot> snapshots = new HashMap<>();

    /** Operator path: resolve/mint the position id, map, and project. */
    public FeedResult feed(GenericRow fillsRow, FillContext ctx, long nowMs) {
        Objects.requireNonNull(fillsRow, "fillsRow");
        Objects.requireNonNull(ctx, "ctx");
        String accountScopeId = fillsRow.getString(FillsColumns.ACCOUNT_SCOPE_ID).toString();
        PositionKey key = new PositionKey(accountScopeId, ctx.instrumentToken(), ctx.side());
        String positionId = resolvePositionId(key);
        Optional<FillEvent> fill = FillEventMapper.mapIfFill(fillsRow, positionId, ctx);
        if (fill.isEmpty()) {
            return FeedResult.notAFill(positionId);
        }
        return feed(fill.get(), nowMs);
    }

    /** Direct path for callers that already hold a resolved {@link FillEvent}. */
    public FeedResult feed(FillEvent fill, long nowMs) {
        Objects.requireNonNull(fill, "fill");
        PositionSnapshot current = snapshots.get(fill.positionId());
        PositionProjector.ProjectionResult r = PositionProjector.apply(current, fill, nowMs);
        switch (r.outcome()) {
            case APPLIED -> snapshots.put(fill.positionId(), r.snapshot());
            case DUPLICATE, STALE, VIOLATION -> {
                // reported to the caller — quarantine/halt is the operator's job
            }
        }
        return toFeedResult(r, fill.positionId());
    }

    private static FeedResult toFeedResult(PositionProjector.ProjectionResult r, String positionId) {
        return switch (r.outcome()) {
            case APPLIED -> FeedResult.applied(r.snapshot(), positionId);
            case DUPLICATE -> FeedResult.duplicate(r.snapshot(), positionId);
            case STALE -> FeedResult.stale(r.snapshot(), positionId);
            case VIOLATION -> FeedResult.violation(positionId, r.reason());
        };
    }

    /**
     * Mints or reuses the position id for a key. Re-entry after a full close
     * mints a NEW id (dossier: "re-entry (new position_id after closure)") —
     * only for a BUY re-open; a SELL against a CLOSED position stays on the
     * closed id and is rejected as an oversell.
     */
    private String resolvePositionId(PositionKey key) {
        String currentId = active.get(key);
        if (currentId == null) {
            return mint(key);
        }
        PositionSnapshot current = snapshots.get(currentId);
        if (current != null && current.state() == PositionState.CLOSED
                && FillEvent.SIDE_BUY.equals(key.side())) {
            return mint(key);
        }
        return currentId;
    }

    private String mint(PositionKey key) {
        int cycle = cycles.merge(key, 1, Integer::sum);
        String id = "pos-" + key.accountScopeId() + "-" + key.instrumentToken()
                + "-" + key.side() + "-" + cycle;
        active.put(key, id);
        return id;
    }

    public PositionSnapshot snapshot(String positionId) {
        return snapshots.get(positionId);
    }

    /** The current active position id for a key (null if never fed). */
    public String positionIdFor(PositionKey key) {
        return active.get(key);
    }

    public int size() {
        return snapshots.size();
    }
}
