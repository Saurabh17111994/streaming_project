package com.trading.ingestion.discontinuity;

import com.trading.ingestion.bridge.BridgeEvent;

/**
 * Minimal suspected-discontinuity evidence surface consumed by
 * {@code IngestionService}.
 *
 * <p>Implemented by the Fluss-backed {@link DiscontinuityWriter} in production
 * and by no-op substitutes in tests (ING-DQ-010) so the service can be
 * constructed and driven without a reachable Fluss. The reason vocabulary and
 * the last-tick snapshot type stay on {@link DiscontinuityWriter} — the shared
 * types documented across the dossiers — so this interface references the
 * concrete class for the vocabulary only, never for behavior.
 */
public interface DiscontinuitySink extends AutoCloseable {

    /** Connection-wide discontinuity (bridge crash, reconnect, time jump). */
    void write(DiscontinuityWriter.Reason reason, String note,
               DiscontinuityWriter.LastTickSnapshot before);

    /** Instrument-scoped discontinuity. */
    void write(DiscontinuityWriter.Reason reason, String note,
               DiscontinuityWriter.LastTickSnapshot before,
               Long instrumentToken, String exchange, String symbol);

    /** Map a bridge lifecycle event to discontinuity evidence. */
    void writeBridgeEvent(BridgeEvent event, DiscontinuityWriter.LastTickSnapshot before);

    @Override
    void close();
}
