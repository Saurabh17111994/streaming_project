package com.trading.ingestion.quarantine;

/**
 * Minimal quarantine-evidence surface consumed by {@code IngestionService}.
 *
 * <p>Implemented by the Fluss-backed {@link QuarantineWriter} in production
 * and by no-op substitutes in tests (ING-DQ-010) so the service can be
 * constructed and driven without a reachable Fluss. The reason vocabulary
 * stays on {@link QuarantineWriter.Reason} — the shared classification enum
 * documented across the dossiers — so this interface references the concrete
 * class for the vocabulary only, never for behavior.
 */
public interface QuarantineSink extends AutoCloseable {

    /** Connection-wide quarantine (no instrument context). */
    void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail);

    /** Quarantine with optional instrument context. */
    void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
               Long instrumentToken, String exchange, String symbol);

    @Override
    void close();
}
