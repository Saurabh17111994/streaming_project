package com.trading.compute.signaljob;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;

/**
 * SCH-19 dual-sink machinery (machinery only — the feed is the ranking stage,
 * Slice 3): attaches the two sinks every published decision must reach, both
 * wrapped in {@link StallGuardedSink} (tracker 14 box 682/116 — the Fluss
 * client's unbounded hang points are bounded on every sink):
 *
 * <ol>
 *   <li><b>{@code Trade_Decisions} LOG</b> — append-only
 *       {@code RowDataSerializationSchema(true, true)}: every published
 *       decision is one immutable instruction row (REQ-FLS-008);</li>
 *   <li><b>{@code trade_instruction_state} KV index</b> — the 4-column index
 *       row (via {@link TradeDecisionIndexMapper}, which recomputes the
 *       canonical content hash) upserted with
 *       {@code RowDataSerializationSchema(false, false)}: the durable
 *       instruction → hash record the instruction-feed protocol checks before
 *       every append (REQ-FLS-015; DEC-038: Fluss is the authoritative index,
 *       rebuildable from the LOG).</li>
 * </ol>
 *
 * <p>Both sinks carry pinned UIDs ({@code trade-decisions-sink},
 * {@code trade-instruction-state-sink}) so their checkpoint-restore anchors
 * survive topology changes (CHECKPOINT-RESTORE-001 habit). Partial visibility
 * between the two sinks is reconciled by {@code instruction_id}
 * (SIG-INT-002 pattern).
 */
public final class TradeDecisionsSinks {

    private TradeDecisionsSinks() {}

    /**
     * Attach the LOG + KV-index dual sinks to the decision stream. The stream
     * is consumed twice (fan-out); both sinks sit inside the stall guard.
     */
    public static void attach(DataStream<RowData> decisions, SignalJobConfig config) {
        // (a) immutable instruction LOG — append-only
        decisions
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.tradeDecisionsTable())
                                .setSerializationSchema(new RowDataSerializationSchema(true, true))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        config.sinkWriteStallTimeoutMs()))
                .name("trade-decisions-sink")
                .uid("trade-decisions-sink");

        // (b) instruction-hash KV index — upsert, canonical hash recomputed
        decisions
                .map(new TradeDecisionIndexMapper())
                .name("trade-instruction-index-map")
                .uid("trade-instruction-index-map")
                .sinkTo(new StallGuardedSink<>(
                        FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.tradeInstructionStateTable())
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                .setOption("client.writer.retries", "2")
                                .build(),
                        config.sinkWriteStallTimeoutMs()))
                .name("trade-instruction-state-sink")
                .uid("trade-instruction-state-sink");
    }
}
