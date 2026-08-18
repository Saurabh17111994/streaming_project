package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts valid signal candidates to immutable execution-intent rows.
 *
 * <p>This operator performs no network I/O and has no broker authority. It is
 * installed only when {@code EXECUTION_INTENT_ENABLED=true}; invalid or
 * unsupported candidates are counted and dropped here. Durable quarantine and
 * changed-identity enforcement belong to the Java execution gateway (T2).
 */
public final class ExecutionIntentProducerFunction
        extends RichFlatMapFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionIntentProducerFunction.class);

    private final SignalJobConfig config;
    private transient Counter rejectedCounter;

    public ExecutionIntentProducerFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        rejectedCounter = getRuntimeContext().getMetricGroup()
                .counter("compute.execution_intent.rejected");
    }

    @Override
    public void flatMap(RowData candidate, Collector<RowData> out) {
        try {
            String tradeContextId = ExecutionIntentContextResolver.resolveEntry(
                    candidate, config.executionAccountScopeId());
            ExecutionIntent intent = ExecutionIntentBuilder.fromCandidate(
                    candidate,
                    config.executionAccountScopeId(),
                    config.executionPartitionId(),
                    config.executionProductType(),
                    config.executionTimeInForce(),
                    config.configurationVersion(),
                    tradeContextId);
            out.collect(ExecutionIntentBuilder.build(intent));
        } catch (IllegalArgumentException e) {
            if (rejectedCounter != null) {
                rejectedCounter.inc();
            }
            LOG.warn("execution-intent: rejected candidate before executable output: {}",
                    e.getMessage());
        }
    }
}
