package com.trading.execution.gateway;

import java.util.List;
import java.util.function.Consumer;
import org.apache.fluss.row.InternalRow;

/** Point-read/replay boundary for control state; it never mutates OMS state. */
public interface ControlStateStore extends AutoCloseable {
    enum Status { FOUND, NOT_FOUND, UNAVAILABLE }
    record Lookup(Status status, InternalRow row, String detail) {}
    Lookup lookup(String tableName, List<Object> keyFields);
    void replaySafetyHalts(Consumer<InternalRow> consumer);
    @Override void close() throws Exception;

    default Lookup lookup(String tableName, Object... keyFields) {
        return lookup(tableName, List.of(keyFields));
    }
}
