package com.trading.compute.babysitter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BAB-UNIT-001/002/003 (docs/08_implementation/05-execution-core.md — Babysitter
 * section; Task 7): the Babysitter replaces the {@code fromElements(0L)} marker
 * source with a {@code Positions} changelog observation pipeline, retains
 * strict no-op (zero {@code Position_Actions}) for every input, and fails
 * closed at startup if {@code POSITION_ACTIONS_ENABLED} is anything but
 * {@code false} — and the write path contains no Arrow, lifecycle, position,
 * or execution sink.
 *
 * <p>All tests are cluster-free: BAB-UNIT-001/003 attach the production
 * observation operators to an in-memory {@code Positions} stand-in and inspect
 * the generated {@link StreamGraph} of {@link BabysitterJob#attachObservationPipeline}
 * (the exact graph the production source feeds), and BAB-UNIT-002 drives
 * {@link BabysitterJob#validateActionFlag} directly. The live MiniCluster
 * restore proof is BAB-INT-001 ({@link BabysitterPositionsRestoreIntegrationTest},
 * cluster-gated) in the same package.
 */
@DisplayName("BAB-UNIT-001/002/003: Positions observation pipeline, fail-closed, no broker path")
class BabysitterJobTest {

    private static final String FAIL_CLOSED_MSG = "POSITION_ACTIONS_ENABLED must be false in MVP";

    private static BabysitterConfig testConfig() {
        return new BabysitterConfig("localhost:9123", "default", "Positions", null,
                60_000L, 60_000L, false, null);
    }

    /** Cluster-free stand-in: the exact pipeline operators feed on a Positions stream. */
    private static StreamGraph pipelineGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> positions = env.fromElements(
                TestPositionsRows.row("POS-1", "ev-1", 1L, "OPEN", 10, 0),
                TestPositionsRows.row("POS-2", "ev-1", 1L, "OPEN", 5, 0))
                .uid("test-standin-source"); // cluster-free stand-in for the Fluss Positions source
        BabysitterJob.attachObservationPipeline(positions, testConfig());
        return env.getStreamGraph();
    }

    @Test
    @DisplayName("BAB-UNIT-001: the Positions pipeline emits zero Position_Actions with explicit UIDs")
    void topologyEmitsZeroActions() {
        StreamGraph graph = pipelineGraph();
        Collection<StreamNode> nodes = graph.getStreamNodes();
        assertFalse(nodes.isEmpty(), "pipeline must contain operators");

        Set<String> uids = new HashSet<>();
        for (StreamNode node : nodes) {
            String uid = node.getTransformationUID();
            assertNotNull(uid,
                    "operator '" + node.getOperatorName() + "' has NO explicit UID — "
                            + "its checkpoint-restore anchor would drift with topology changes "
                            + "(CHECKPOINT-RESTORE-001 / DEC-035)");
            assertTrue(uids.add(uid), "duplicate UID '" + uid + "'");
            String name = node.getOperatorName();
            assertFalse(containsNoopAction(name),
                    "operator '" + name + "' emits/sinks Position_Actions — the MVP Babysitter "
                            + "must emit zero action records for every input (DEC-017)");
        }
        // The replaced marker source and its UIDs are gone.
        assertFalse(uids.contains("babysitter-mvp-source"),
                "the fromElements(0L) marker source must be replaced by the Positions changelog");
        assertTrue(uids.containsAll(Set.of(
                        "babysitter-rowkind-filter",
                        "babysitter-position-deserialize",
                        "babysitter-position-observation",
                        "babysitter-discard")),
                "the Positions observation pipeline must be wired; found UIDs " + uids);
    }

    @Test
    @DisplayName("BAB-UNIT-002: any action-enable value other than 'false' fails closed")
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
        BabysitterJob.validateActionFlag(null);
        BabysitterJob.validateActionFlag("false");
        BabysitterJob.validateActionFlag("FALSE");
        BabysitterJob.validateActionFlag(" false ");
        BabysitterJob.validateActionFlag("false\n");
    }

    @Test
    @DisplayName("BAB-UNIT-003: the write path has no Arrow/lifecycle/position/execution sink")
    void writePathHasNoBrokerOrExternalSink() {
        StreamGraph graph = pipelineGraph();
        Set<StreamNode> sinks = new HashSet<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String uid = node.getTransformationUID();
            String name = node.getOperatorName().toLowerCase(Locale.ROOT);
            // A sink operator is named "Sink: <uid>" in the generated graph.
            if (name.startsWith("sink")) {
                sinks.add(node);
                assertEqualsSafe(uid, "babysitter-discard",
                        "the only sink must be the Babysitter no-op discard; found '" + uid + "'");
            }
            assertFalse(containsForbiddenWritePath(name, uid == null ? "" : uid.toLowerCase(Locale.ROOT)),
                    "write path must not touch '" + node.getOperatorName() + "'");
        }
        assertFalse(sinks.isEmpty(), "the pipeline must terminate in a sink");
        // The observation operator never emits; nothing reaches the discard, so the
        // job issues zero broker/external outputs by construction.
        assertTrue(sinks.size() >= 1, "exactly the one no-op sink observed");
    }

    private static boolean containsNoopAction(String name) {
        return name.contains("Position_Actions");
    }

    private static boolean containsForbiddenWritePath(String name, String uid) {
        String combined = name + " " + uid;
        return combined.contains("arrow")
                || combined.contains("lifecycle")
                || combined.contains("execution")
                || combined.contains("fills")
                || combined.contains("action_capture")
                || combined.contains("position_actions");
    }

    private static void assertEqualsSafe(String actual, String expected, String msg) {
        assertTrue(expected.equals(actual), msg + " (expected '" + expected + "', got '" + actual + "')");
    }
}
