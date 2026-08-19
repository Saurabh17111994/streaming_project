package com.trading.common.schema.projection;

import com.trading.common.model.PositionState;
import com.trading.common.schema.position.FillEvent;
import com.trading.common.schema.position.PositionProjectorDriver;
import com.trading.common.schema.position.PositionSnapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Offline {@link NautilusPositionAuthority} that routes position arithmetic to
 * the documented Java parity projector ({@link PositionProjectorDriver}) and
 * mints deterministic position ids, standing in for the Rust/Nautilus
 * authority until the live writer exists. It is the test-time bridge that lets
 * the projection serialization path be compared against the reference oracle.
 */
public final class ReferencePositionAuthority implements NautilusPositionAuthority {

    private final PositionProjectorDriver driver = new PositionProjectorDriver();
    private final Map<Key, String> entrySide = new HashMap<>();
    private final Map<Key, Integer> cycles = new HashMap<>();
    private final Map<Key, String> active = new HashMap<>();

    /** Account+instrument position key (the entry side is fixed by the first fill). */
    private record Key(String account, long instrument) {}

    @Override
    public Optional<NautilusPositionEvent> apply(NormalizedPostback postback) {
        if (!postback.isFill()) {
            return Optional.empty();
        }
        Key key = new Key(postback.accountScopeId(), postback.instrumentToken());
        String entrySide = this.entrySide.computeIfAbsent(key,
                k -> postback.side()); // first fill fixes the position's entry side
        String positionId = resolvePositionId(key, entrySide, postback.side());
        FillEvent fill = new FillEvent(
                positionId,
                postback.tradeContextId(),
                postback.accountScopeId(),
                postback.instrumentToken(),
                postback.exchange(),
                postback.symbol(),
                postback.side(),
                postback.fillQty(),
                postback.fillPricePaise(),
                postback.sourceEventId(),
                postback.sourceSequence(),
                postback.eventTimeMs());
        PositionProjectorDriver.FeedResult r = driver.feed(fill, postback.receiveTimeMs());
        if (r.outcome() != PositionProjectorDriver.FeedOutcome.APPLIED) {
            return Optional.empty();
        }
        PositionSnapshot s = r.snapshot();
        return Optional.of(new NautilusPositionEvent(
                s.positionId(), s.tradeContextId(), s.accountScopeId(), s.instrumentToken(),
                s.exchange(), s.symbol(), s.side(), s.state(), s.openQuantity(),
                s.closedQuantity(), s.averageEntryPaise(), s.averageExitPaise(),
                s.sourceEventId(), s.sourceVersion(), s.lastUpdateTs()));
    }

    private String resolvePositionId(Key key, String entrySide, String fillSide) {
        String currentId = active.get(key);
        if (currentId == null) {
            return mint(key, entrySide);
        }
        PositionSnapshot current = driver.snapshot(currentId);
        if (current != null && current.state() == PositionState.CLOSED
                && FillEvent.SIDE_BUY.equals(fillSide)) {
            return mint(key, FillEvent.SIDE_BUY); // fresh BUY re-opens a new cycle
        }
        return currentId;
    }

    private String mint(Key key, String side) {
        int cycle = cycles.merge(key, 1, Integer::sum);
        String id = "pos-" + key.account() + "-" + key.instrument() + "-" + side + "-" + cycle;
        active.put(key, id);
        return id;
    }

    public PositionProjectorDriver oracle() {
        return driver;
    }
}
