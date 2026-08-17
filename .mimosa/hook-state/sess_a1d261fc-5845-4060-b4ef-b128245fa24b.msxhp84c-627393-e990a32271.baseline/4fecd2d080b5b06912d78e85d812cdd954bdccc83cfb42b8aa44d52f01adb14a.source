package com.trading.compute.babysitter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BAB-UNIT-001 / BAB-UNIT-002 (docs/08_implementation/06-babysitter.md,
 * BABYSITTER-001) — the MVP Babysitter contract (DEC-017): a strict no-op
 * that emits zero {@code Position_Actions} for every input and fails closed
 * at startup if {@code POSITION_ACTIONS_ENABLED} is anything but {@code
 * false}.
 *
 * <p>Both tests are cluster-free: BAB-UNIT-001 inspects the generated
 * {@link StreamGraph} of {@link BabysitterJob#buildTopology()} (the same
 * graph the job submits), and BAB-UNIT-002 drives
 * {@link BabysitterJob#validateActionFlag} directly with every env-var
 * variant. BAB-INT-001, BAB-HARNESS-001, BAB-FAIL-001/002, and BAB-OPS-001
 * stay pending until the real Positions-changelog source and observation
 * state land (06-babysitter.md).
 */
@DisplayName("BAB-UNIT-001/002: MVP Babysitter strict no-op and action-enable fail-closed")
class BabysitterJobTest {

    private static final String FAIL_CLOSED_MSG = "POSITION_ACTIONS_ENABLED must be false in MVP";

    @Test
    @DisplayName("BAB-UNIT-001: the submitted topology emits zero Position_Actions (only marker + discard)")
    void topologyEmitsZeroActions() {
        StreamGraph graph = BabysitterJob.buildTopology().getStreamGraph();
        Collection<StreamNode> nodes = graph.getStreamNodes();
        assertFalse(nodes.isEmpty(), "MVP topology must contain operators");

        Set<String> uids = new HashSet<>();
        for (StreamNode node : nodes) {
            String uid = node.getTransformationUID();
            assertNotNull(uid,
                    "operator '" + node.getOperatorName() + "' has NO explicit UID — "
                            + "its checkpoint-restore anchor would drift with topology changes "
                            + "(CHECKPOINT-RESTORE-001 / DEC-035)");
            assertTrue(uids.add(uid), "duplicate UID '" + uid + "'");
            String name = node.getOperatorName();
            assertFalse(name.contains("Position_Actions"),
                    "operator '" + name + "' emits/sinks Position_Actions — the MVP Babysitter "
                            + "must emit zero action records for every input (DEC-017)");
        }
        assertTrue(uids.containsAll(Set.of("babysitter-mvp-marker", "babysitter-mvp-discard")),
                "the no-op marker -> discard topology must be wired; found UIDs " + uids);
    }

    @Test
    @DisplayName("BAB-UNIT-002: any action-enable value other than 'false' fails closed at startup")
    void actionFlagFailsClosed() {
        for (String bad : new String[] {"true", "TRUE", "True", "yes", "1", "on", "", "  "}) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> BabysitterJob.validateActionFlag(bad),
                    "value '" + bad + "' must fail closed in MVP");
            assertTrue(e.getMessage().contains(FAIL_CLOSED_MSG),
                    "error must name the flag, got: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("BAB-UNIT-002: unset or 'false' (trimmed, any case) starts normally")
    void actionFlagAcceptsUnsetOrFalse() {
        BabysitterJob.validateActionFlag(null);       // unset — the normal MVP run
        BabysitterJob.validateActionFlag("false");
        BabysitterJob.validateActionFlag("FALSE");
        BabysitterJob.validateActionFlag(" false ");  // R-286: config-file exports carry padding
        BabysitterJob.validateActionFlag("false\n");  // R-286: ... and trailing newlines
    }
}
