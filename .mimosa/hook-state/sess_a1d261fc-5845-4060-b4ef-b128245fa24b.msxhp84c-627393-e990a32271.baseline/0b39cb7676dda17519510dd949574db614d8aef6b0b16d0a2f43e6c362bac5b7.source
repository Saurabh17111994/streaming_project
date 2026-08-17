package com.trading.common.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Slot-scoped safety state machine (plan.md &sect; "Slot-scoped safety
 * propagation"): UNSAFE&rarr;RECOVERED with strict-epoch semantics, the five
 * ignore gates, and token-scoped suppression.
 */
@DisplayName("SafetyStateTracker: transition rules + gates")
class SafetyStateTrackerTest {

    // hft-0 = [1,2,3], hft-1 = [4,5,6] (2 slots, connection limit 3).
    private SlotAssignmentResolver assignment;
    private SafetyStateTracker tracker;

    @BeforeEach
    void setUp() {
        assignment = SlotAssignmentResolver.of(List.of(1L, 2L, 3L, 4L, 5L, 6L), 2, 3);
        tracker = new SafetyStateTracker(assignment);
    }

    @Test
    @DisplayName("UNSAFE opens the suppression window for the slot's tokens only")
    void unsafeSuppressesSlotTokensOnly() {
        assertEquals(SafetyStateTracker.ApplyResult.NEW_UNSAFE,
                tracker.apply(row("hft-0", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED")));
        assertTrue(tracker.isUnsafe("hft-0"));
        assertTrue(tracker.isTokenSuppressed(1L));
        assertTrue(tracker.isTokenSuppressed(3L));
        assertFalse(tracker.isTokenSuppressed(4L));  // healthy slot continues
        assertFalse(tracker.isTokenSuppressed(999L)); // unassigned token
        assertEquals(1L, tracker.stateOf("hft-0").connectionEpoch());
        assertEquals("FEED_STALLED", tracker.stateOf("hft-0").reasonCode());
    }

    @Test
    @DisplayName("equal-epoch re-delivery is a duplicate; lower epoch is stale")
    void duplicateAndStaleUnsafe() {
        tracker.apply(row("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED"));
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_DUPLICATE,
                tracker.apply(row("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED")));
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_STALE_EPOCH,
                tracker.apply(row("hft-0", 4L, SlotSafetyStatus.UNSAFE, "FEED_STALLED")));
        assertEquals(5L, tracker.stateOf("hft-0").connectionEpoch());
    }

    @Test
    @DisplayName("higher-epoch UNSAFE while unsafe advances the state (new instance)")
    void newerUnsafeAdvances() {
        tracker.apply(row("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED"));
        assertEquals(SafetyStateTracker.ApplyResult.NEW_UNSAFE,
                tracker.apply(row("hft-0", 7L, SlotSafetyStatus.UNSAFE, "HEARTBEAT_FAILED")));
        assertEquals(7L, tracker.stateOf("hft-0").connectionEpoch());
        assertEquals("HEARTBEAT_FAILED", tracker.stateOf("hft-0").reasonCode());
        assertTrue(tracker.isUnsafe("hft-0"));
    }

    @Test
    @DisplayName("RECOVERED requires a prior UNSAFE and strictly greater epoch")
    void recoveredRules() {
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_NO_PRIOR_UNSAFE,
                tracker.apply(row("hft-0", 5L, SlotSafetyStatus.RECOVERED, "")));

        tracker.apply(row("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED"));
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_STALE_EPOCH,
                tracker.apply(row("hft-0", 4L, SlotSafetyStatus.RECOVERED, "")));
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_STALE_EPOCH,
                tracker.apply(row("hft-0", 5L, SlotSafetyStatus.RECOVERED, "")));

        assertEquals(SafetyStateTracker.ApplyResult.RECOVERED,
                tracker.apply(row("hft-0", 6L, SlotSafetyStatus.RECOVERED, "")));
        assertFalse(tracker.isUnsafe("hft-0"));
        assertFalse(tracker.isTokenSuppressed(1L));
        assertEquals(SlotSafetyStatus.RECOVERED, tracker.stateOf("hft-0").status());
        assertEquals(6L, tracker.stateOf("hft-0").connectionEpoch());

        // Recovery without an unsafe to clear (again) is a no-op.
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_NO_PRIOR_UNSAFE,
                tracker.apply(row("hft-0", 7L, SlotSafetyStatus.RECOVERED, "")));
    }

    @Test
    @DisplayName("unsafe→recovered→unsafe re-opens at the newer epoch")
    void reopenAfterRecovery() {
        tracker.apply(row("hft-0", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED"));
        tracker.apply(row("hft-0", 2L, SlotSafetyStatus.RECOVERED, ""));
        assertEquals(SafetyStateTracker.ApplyResult.NEW_UNSAFE,
                tracker.apply(row("hft-0", 3L, SlotSafetyStatus.UNSAFE, "BRIDGE_EXIT")));
        assertTrue(tracker.isUnsafe("hft-0"));
        assertEquals(3L, tracker.stateOf("hft-0").connectionEpoch());
    }

    @Test
    @DisplayName("gates: unknown slot, hash mismatch, manifest mismatch")
    void trustGates() {
        String badHash = "0".repeat(64);
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_UNKNOWN_SLOT,
                tracker.apply(row("hft-9", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED",
                        "a".repeat(64))));

        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_HASH_MISMATCH,
                tracker.apply(row("hft-0", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED", badHash)));

        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_MANIFEST_MISMATCH,
                tracker.apply(row("hft-0", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED",
                        assignment.tokenSetHashOf("hft-0"), badHash)));

        // None of the rejected rows may open a suppression window.
        assertFalse(tracker.isUnsafe("hft-0"));
    }

    @Test
    @DisplayName("gates: source component and contract version")
    void provenanceGates() {
        SlotSafetyRequest otherComponent = new SlotSafetyRequest(
                "req-1", "COMPUTE", "hft-0", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED",
                assignment.manifestFingerprint(), assignment.tokenSetHashOf("hft-0"), 1L, 2);
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_SOURCE_COMPONENT,
                tracker.apply(otherComponent));

        SlotSafetyRequest oldVersion = new SlotSafetyRequest(
                "req-2", SlotSafetyRequest.SOURCE_COMPONENT_INGESTION, "hft-0", 1L,
                SlotSafetyStatus.UNSAFE, "FEED_STALLED",
                assignment.manifestFingerprint(), assignment.tokenSetHashOf("hft-0"), 1L, 1);
        assertEquals(SafetyStateTracker.ApplyResult.IGNORED_CONTRACT_VERSION,
                tracker.apply(oldVersion));

        assertFalse(tracker.isUnsafe("hft-0"));
        assertNull(tracker.stateOf("hft-0"));
    }

    @Test
    @DisplayName("snapshot is an immutable copy that reflects applied transitions")
    void snapshot() {
        tracker.apply(row("hft-0", 1L, SlotSafetyStatus.UNSAFE, "FEED_STALLED"));
        var snap = tracker.snapshot();
        assertEquals(1, snap.size());
        assertEquals(SlotSafetyStatus.UNSAFE, snap.get("hft-0").status());
        // Snapshot is detached: later transitions do not leak into it.
        tracker.apply(row("hft-0", 2L, SlotSafetyStatus.RECOVERED, ""));
        assertTrue(snap.get("hft-0").status() == SlotSafetyStatus.UNSAFE);
    }

    private SlotSafetyRequest row(String slotId, long epoch, SlotSafetyStatus status, String reason) {
        return row(slotId, epoch, status, reason, assignment.tokenSetHashOf(slotId));
    }

    private SlotSafetyRequest row(String slotId, long epoch, SlotSafetyStatus status,
                                  String reason, String slotHash) {
        return row(slotId, epoch, status, reason, slotHash, assignment.manifestFingerprint());
    }

    private SlotSafetyRequest row(String slotId, long epoch, SlotSafetyStatus status,
                                  String reason, String slotHash, String manifestHash) {
        return new SlotSafetyRequest(
                "req-" + slotId + "-" + epoch + "-" + status,
                SlotSafetyRequest.SOURCE_COMPONENT_INGESTION,
                slotId, epoch, status, reason, manifestHash, slotHash, epoch * 1000L, 2);
    }
}
