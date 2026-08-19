package com.trading.common.schema.projection;

import java.util.ArrayList;
import java.util.List;

/** Pure-JVM immutable quarantine LOG (offline tests/drills). */
public final class InMemoryPostbackQuarantineStore implements PostbackQuarantineStore {

    private final List<QuarantinedPostback> rows = new ArrayList<>();

    @Override
    public void append(QuarantinedPostback row) {
        rows.add(row);
    }

    @Override
    public List<QuarantinedPostback> all() {
        return List.copyOf(rows);
    }

    public int size() {
        return rows.size();
    }
}
