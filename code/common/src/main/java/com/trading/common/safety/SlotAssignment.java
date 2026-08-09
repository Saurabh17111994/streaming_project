package com.trading.common.safety;

import java.util.List;

/**
 * Deterministic manifest-derived slot assignment for safety suppression
 * (plan.md &sect; "Slot-scoped safety propagation", rule 4).
 *
 * <p>The tracker consults this to (a) resolve which slot owns a token and
 * (b) verify that an incoming safety request's {@code assigned_token_set_hash}
 * matches the hash of the tokens deterministically assigned to that slot.
 * A request whose hash disagrees with the manifest-derived assignment is not
 * trusted (its slot could have been claimed by a different manifest).
 */
public interface SlotAssignment {

    /** All slot ids, e.g. {@code ["hft-0", "hft-1"]}. */
    List<String> slotIds();

    /**
     * Slot that owns {@code token}, or {@code null} if the token is not part
     * of this assignment (not subscribed under the manifest).
     */
    String slotIdOf(long token);

    /**
     * {@code TokenSetHash} of the tokens assigned to {@code slotId}, or
     * {@code null} for an unknown slot.
     */
    String tokenSetHashOf(String slotId);

    /** {@code TokenSetHash} of the full manifest token set. */
    String manifestFingerprint();
}
