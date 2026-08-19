package com.trading.common.schema.position;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** {@link PositionsStateStore} backed by an in-memory map — pure-JVM twin of
 * the Fluss store for unit tests and drills. */
public final class InMemoryPositionsStateStore implements PositionsStateStore {

    private final Map<String, PositionSnapshot> byPositionId = new HashMap<>();

    @Override
    public PositionSnapshot lookup(String positionId) {
        return byPositionId.get(positionId);
    }

    @Override
    public void upsert(PositionSnapshot snapshot) {
        byPositionId.put(Objects.requireNonNull(snapshot, "snapshot").positionId(), snapshot);
    }

    public int size() {
        return byPositionId.size();
    }

    /** All snapshots sorted by position id (deterministic introspection). */
    public List<PositionSnapshot> all() {
        return byPositionId.values().stream()
                .sorted(Comparator.comparing(PositionSnapshot::positionId))
                .toList();
    }
}
