package com.trading.compute.signaljob;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Durable-write cadence for DEC-038 first-seen fingerprints
 * (docs/08_implementation/04-signal-job.md §Design — write cadence):
 * the {@code fingerprint_dedup} KV upserts are batched — flush when
 * {@code DEDUP_WRITE_BATCH_SIZE} rows accumulate OR a processing-time timer
 * at {@code DEDUP_WRITE_BATCH_MS} fires, whichever first. The downstream
 * {@code FlussSink} additionally aligns to checkpoint barriers, so the worst
 * durable-write window is bounded by {@code DEDUP_WRITE_BATCH_MS} plus the
 * sink's own batching.
 *
 * <p>Keyed on a constant so the operator is a single instance (parallelism 1)
 * with a single in-memory buffer — no keyed state, nothing to restore. Rows
 * pass through unchanged (they are already in {@code fingerprint_dedup}
 * layout; the sink maps INSERT to UPSERT).
 */
public class FingerprintDedupWriterFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final SignalJobConfig config;

    private transient List<RowData> buffer;
    private transient long pendingSinceMs;

    public FingerprintDedupWriterFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        buffer = new ArrayList<>();
        pendingSinceMs = -1L;
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out) {
        if (pendingSinceMs < 0) {
            pendingSinceMs = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(
                    pendingSinceMs + config.dedupWriteBatchMs());
        }
        buffer.add(row);
        if (buffer.size() >= config.dedupWriteBatchSize()) {
            flush(ctx, out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) {
        flush(ctx, out);
    }

    private void flush(Context ctx, Collector<RowData> out) {
        if (buffer.isEmpty()) {
            return;
        }
        for (RowData row : buffer) {
            out.collect(row);
        }
        buffer.clear();
        pendingSinceMs = -1L;
    }
}
