package com.trading.ingestion.safety;

/**
 * Minimal safety-halt evidence surface consumed by {@code IngestionService}.
 *
 * <p>Implemented by the Fluss-backed {@link SafetyHaltWriter} in production
 * and by no-op substitutes in tests (ING-DQ-010) so the service can be
 * constructed and driven without a reachable Fluss. The state and reason-code
 * vocabularies stay on {@link SafetyHaltWriter} — the shared types documented
 * across the dossiers — so this interface references the concrete class for
 * the vocabulary only, never for behavior.
 */
public interface SafetySink extends AutoCloseable {

    /**
     * Write one safety request row.
     *
     * @return the computed halt_request_id (for caller-side dedup)
     */
    String write(String slotId, long connectionEpoch, SafetyHaltWriter.SafetyState state,
                 SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                 String evidenceReference, long detectedTsMs);

    @Override
    void close();
}
