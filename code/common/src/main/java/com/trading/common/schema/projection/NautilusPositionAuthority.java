package com.trading.common.schema.projection;

import java.util.Optional;

/**
 * The position arithmetic authority (T6, CHG-045). In production this is
 * Rust/Nautilus (the only position/PnL calculator). The projection layer calls
 * it to obtain the authoritative post-fill position and only serializes the
 * returned event — it never computes quantities itself. The offline parity
 * test supplies the documented Java {@code PositionProjectorDriver} as the
 * authority to prove the projection serialization agrees with the reference.
 *
 * <p>The authority owns its own position state (minting ids, tracking cycles)
 * — exactly as Nautilus owns production position state — and returns the
 * authoritative post-fill event carrying the resolved {@code positionId} with
 * the stable {@code sourceSequence}.
 */
public interface NautilusPositionAuthority {

    /**
     * @param postback the normalized postback (the fill driving the update)
     * @return the authoritative Nautilus position event (with resolved
     *         positionId), or empty when the postback is not a fill or the
     *         authority produces no position update
     */
    Optional<NautilusPositionEvent> apply(NormalizedPostback postback);
}
