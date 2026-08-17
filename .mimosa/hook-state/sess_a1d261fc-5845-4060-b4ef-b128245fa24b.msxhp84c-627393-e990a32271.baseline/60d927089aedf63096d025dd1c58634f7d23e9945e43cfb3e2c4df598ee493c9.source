package com.trading.common.safety;

/**
 * Immutable per-slot safety state held by the {@link SafetyStateTracker}.
 * A slot is suppressed while {@code status == UNSAFE}; a {@code RECOVERED}
 * state records the connection epoch at which the slot became safe again,
 * which is the boundary for admitting post-recovery input.
 *
 * @param slotId           {@code "hft-N"}
 * @param connectionEpoch  epoch of the transition that produced this state
 * @param status           UNSAFE (suppressed) or RECOVERED (safe boundary)
 * @param reasonCode       reason enum name for UNSAFE; {@code ""} for RECOVERED
 * @param detectionTimeMs  epoch milliseconds of the detection
 * @param tokenSetHash     {@code TokenSetHash} of the slot's assigned tokens
 */
public record SlotSafetyState(
        String slotId,
        long connectionEpoch,
        SlotSafetyStatus status,
        String reasonCode,
        long detectionTimeMs,
        String tokenSetHash
) {}
