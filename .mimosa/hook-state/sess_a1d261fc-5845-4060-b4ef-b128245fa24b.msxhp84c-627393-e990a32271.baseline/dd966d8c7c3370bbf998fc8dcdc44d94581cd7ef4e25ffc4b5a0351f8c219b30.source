package com.trading.ingestion.write;

import com.trading.ingestion.model.TickPacket;
import java.util.concurrent.CompletableFuture;

/**
 * Converts a {@link TickPacket} into a Fluss row and appends it.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@code RealFlussRowConverter} — production: uses
 *       {@code org.apache.fluss.client.table.writer.AppendWriter}</li>
 *   <li>{@code StubFlussRowConverter} — development: acknowledges without real I/O</li>
 * </ul>
 */
public interface FlussRowConverter extends AutoCloseable {

    /**
     * Convert {@code packet} to a Fluss row and append.
     * Returns a future that completes when Fluss acknowledges.
     */
    CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet);

    /**
     * Estimate the on-wire row size in bytes for backpressure accounting.
     */
    int estimatedRowSize(TickPacket packet);
}
