package com.trading.compute.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.trading.compute.signaljob.SignalJob;
import com.trading.compute.signaljob.SignalJobConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline JobGraph / operator-ID dump for checkpoint-restore compatibility
 * evidence (CANDLE-KV-REPLAY-001 P6).
 *
 * <p>Builds the exact SignalJob topology via {@link SignalJob#buildTopology} —
 * the same code path a running job uses — <b>without executing it</b> (no
 * task deployment, no checkpoints, no table writes; the Fluss source/sink
 * builders perform read-only metadata lookups, so the target cluster must be
 * reachable, exactly like a real startup). Writes three artifacts into the
 * output directory:
 *
 * <ul>
 *   <li>{@code stream-nodes.txt} — every stream operator: {@code id | operator name}</li>
 *   <li>{@code job-vertices.txt} — every job vertex (post-chaining):
 *       {@code name | vertex id | parallelism}. Job-vertex IDs are the
 *       restore-compatibility contract: an unchanged vertex ID set with
 *       unchanged operator state shapes is what makes a checkpoint from a
 *       previous run restorable.</li>
 *   <li>{@code jobgraph.json} — the full JSON plan (JsonPlanGenerator).</li>
 * </ul>
 *
 * <p>Usage: {@code java -cp <compute classes>:<flink dist libs>}
 * {@code com.trading.compute.tools.JobGraphDump [out-dir]}. The config is read
 * from the environment exactly like the job ({@code SignalJobConfig.fromEnv});
 * since the fail-closed startup gate (A3.3) applies, the environment MUST
 * provide either {@code STATE_RECOVERY_PATH} or {@code ALLOW_FULL_REPLAY=true}.
 */
public final class JobGraphDump {

    private static final Logger LOG = LoggerFactory.getLogger(JobGraphDump.class);

    private JobGraphDump() {}

    public static void main(String[] args) throws Exception {
        Path outDir = args.length > 0 ? Path.of(args[0]) : Path.of("logs/candle-kv-replay-001/jobgraph");
        Files.createDirectories(outDir);

        SignalJobConfig config = SignalJobConfig.fromEnv();
        LOG.info("jobgraph-dump: dumping graph for config {}", config);

        StreamExecutionEnvironment env = SignalJob.buildTopology(config);
        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();

        writeStreamNodes(outDir, streamGraph);
        writeJobVertices(outDir, jobGraph);
        writeJsonPlan(outDir, jobGraph);

        LOG.info("jobgraph-dump: wrote stream-nodes.txt, job-vertices.txt, jobgraph.json to {}", outDir);
    }

    private static void writeStreamNodes(Path outDir, StreamGraph graph) throws Exception {
        String content = graph.getStreamNodes().stream()
                .sorted(Comparator.comparingInt(StreamNode::getId))
                .map(n -> n.getId() + " | " + n.getOperatorName())
                .collect(Collectors.joining("\n")) + "\n";
        Files.writeString(outDir.resolve("stream-nodes.txt"), content, StandardCharsets.UTF_8);
    }

    private static void writeJobVertices(Path outDir, JobGraph graph) throws Exception {
        java.util.List<JobVertex> vertices = new java.util.ArrayList<>();
        graph.getVertices().forEach(vertices::add);
        String content = vertices.stream()
                .sorted(Comparator.comparing(JobVertex::getName))
                .map(v -> v.getName() + " | " + v.getID() + " | parallelism=" + v.getParallelism())
                .collect(Collectors.joining("\n")) + "\n";
        Files.writeString(outDir.resolve("job-vertices.txt"), content, StandardCharsets.UTF_8);
    }

    private static void writeJsonPlan(Path outDir, JobGraph graph) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        String json = mapper.writeValueAsString(JsonPlanGenerator.generatePlan(graph));
        Files.writeString(outDir.resolve("jobgraph.json"), json, StandardCharsets.UTF_8);
    }
}
