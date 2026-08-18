package com.trading.execution.gateway;

/** Durable projection boundary. Implementations must observe every Fluss future. */
public interface ProjectionWriter extends AutoCloseable {
    void writeAudit(NormalizedExecutionEvent event) throws Exception;
    void writeLifecycle(NormalizedExecutionEvent event) throws Exception;
    void writePosition(NormalizedExecutionEvent event) throws Exception;
    default void write(NormalizedExecutionEvent event) throws Exception {
        writeAudit(event);
        writeLifecycle(event);
        writePosition(event);
    }
    @Override void close() throws Exception;
}
