package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * DEC-038 durable-write cadence (docs/08_implementation/04-signal-job.md
 * §Design — write cadence): {@link FingerprintDedupWriterFunction} flushes the
 * buffered {@code fingerprint_dedup} rows when {@code DEDUP_WRITE_BATCH_SIZE}
 * rows accumulate OR the {@code DEDUP_WRITE_BATCH_MS} processing-time timer
 * fires, whichever first — the worst durable-write window is bounded by the
 * batch cadence + the downstream sink's own batching.
 */
class FingerprintDedupWriterFunctionTest {

    private static final long T0 = 1_700_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openWith(Map<String, String> extra) throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        env.putAll(extra);
        SignalJobConfig config = SignalJobConfig.from(env);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new FingerprintDedupWriterFunction(config),
                row -> 0L,
                Types.LONG);
        harness.open();
    }

    private void process(RowData row) throws Exception {
        harness.processElement(row, T0);
    }

    private long emitted() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord).count();
    }

    @Test
    void flushOnBatchSize() throws Exception {
        openWith(Map.of("DEDUP_WRITE_BATCH_SIZE", "2",
                "DEDUP_WRITE_BATCH_MS", "60000"));
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        assertEquals(0, emitted(), "below batch size — buffered, not flushed");
        process(TestRawRows.row(2L, T0, "fp-2", "TRADE", 100, 2));
        assertEquals(2, emitted(), "batch size reached — flush");
    }

    @Test
    void flushOnTimer() throws Exception {
        openWith(Map.of("DEDUP_WRITE_BATCH_SIZE", "5000",
                "DEDUP_WRITE_BATCH_MS", "250"));
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        assertEquals(0, emitted(), "buffered until the timer");
        harness.setProcessingTime(250L);
        assertEquals(1, emitted(), "timer fired — flush");
    }

    @Test
    void emptyBufferTimerNoOp() throws Exception {
        openWith(Map.of("DEDUP_WRITE_BATCH_SIZE", "5000",
                "DEDUP_WRITE_BATCH_MS", "250"));
        harness.setProcessingTime(1_000L);
        assertEquals(0, emitted(), "an idle timer must emit nothing");
    }
}
