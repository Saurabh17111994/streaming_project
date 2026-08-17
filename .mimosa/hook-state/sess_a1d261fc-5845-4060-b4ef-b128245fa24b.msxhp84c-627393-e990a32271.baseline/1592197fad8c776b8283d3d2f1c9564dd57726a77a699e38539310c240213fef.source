package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 box 682/116 (2026-08-12): the {@link StallGuardedSink} watchdog
 * must bound EVERY writer call (write/flush/writeWatermark/close) at the
 * configured window — a hanging Fluss client write then fails the task
 * instead of cycling FAILING→RESTARTING forever — while adding zero behavior
 * on the healthy path (pure delegation, no reordering, no drops).
 *
 * <p>Also covers the governed-pin contract:
 * {@code SINK_WRITE_STALL_TIMEOUT_MS} must be strictly positive — a
 * non-positive bound would re-introduce the unbounded hang.
 */
@DisplayName("StallGuardedSink write-path watchdog (tracker 14 box 682/116)")
class StallGuardedSinkTest {

    /** Fake writer whose write/flush/close block for {@code blockMs}. */
    private static final class BlockingWriter implements SinkWriter<String> {
        final List<String> written = new ArrayList<>();
        final List<Boolean> flushes = new ArrayList<>();
        boolean closed;
        boolean watermarkSeen;
        final long blockMs;

        BlockingWriter(long blockMs) {
            this.blockMs = blockMs;
        }

        @Override
        public void write(String element, Context context)
                throws IOException, InterruptedException {
            written.add(element);
            if (blockMs > 0) {
                Thread.sleep(blockMs);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            flushes.add(endOfInput);
            if (blockMs > 0) {
                Thread.sleep(blockMs);
            }
        }

        @Override
        public void writeWatermark(Watermark watermark) {
            watermarkSeen = true;
        }

        @Override
        public void close() throws Exception {
            closed = true;
            if (blockMs > 0) {
                Thread.sleep(blockMs);
            }
        }
    }

    private static final class FakeSink implements Sink<String> {
        final BlockingWriter writer = new BlockingWriter(0);
        long blockMs;

        FakeSink blockFor(long ms) {
            this.blockMs = ms;
            return this;
        }

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            return blockMs > 0 ? new BlockingWriter(blockMs) : writer;
        }
    }

    /** SupportsPreWriteTopology fake that counts pre-write topology invocations. */
    private static final class TopologyCountingSink
            implements Sink<String>, SupportsPreWriteTopology<String> {
        int preWriteCalls;

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            return new BlockingWriter(0);
        }

        @Override
        public org.apache.flink.streaming.api.datastream.DataStream<String> addPreWriteTopology(
                org.apache.flink.streaming.api.datastream.DataStream<String> input) {
            preWriteCalls++;
            return input;
        }
    }

    @Test
    @DisplayName("healthy path: write/flush/close/watermark forwarded, order preserved, no drops")
    void healthyWritePassesThrough() throws Exception {
        FakeSink fake = new FakeSink();
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 5_000L);

        SinkWriter<String> writer = guarded.createWriter(null);
        writer.write("a", null);
        writer.write("b", null);
        writer.write("c", null);
        writer.writeWatermark(new Watermark(1_000L));
        writer.flush(true);
        writer.close();

        assertEquals(List.of("a", "b", "c"), fake.writer.written,
                "guard must never reorder or drop rows");
        assertEquals(List.of(true), fake.writer.flushes);
        assertTrue(fake.writer.watermarkSeen, "writeWatermark must be forwarded");
        assertTrue(fake.writer.closed, "close must be forwarded");
    }

    @Test
    @DisplayName("write exceeding the window throws RuntimeException naming the stalled op")
    void slowWriteExceedsWindowThrows() throws Exception {
        FakeSink fake = new FakeSink().blockFor(300L);
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 100L);
        SinkWriter<String> writer = guarded.createWriter(null);

        RuntimeException e =
                assertThrows(RuntimeException.class, () -> writer.write("x", null),
                        "a write that exceeds the stall window must fail the task");
        assertTrue(e.getMessage().contains("write"), "error must name the stalled op: " + e);
        assertTrue(e.getMessage().contains("stall"), "error must identify the guard: " + e);
    }

    @Test
    @DisplayName("flush exceeding the window throws — bounded even when write itself returns fast")
    void slowFlushExceedsWindowThrows() throws Exception {
        FakeSink fake = new FakeSink().blockFor(300L);
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 100L);
        SinkWriter<String> writer = guarded.createWriter(null);

        assertThrows(RuntimeException.class, () -> writer.flush(true),
                "a flush that exceeds the stall window must fail the task");
    }

    @Test
    @DisplayName("close exceeding the window throws — a hanging writer close cannot hang the task")
    void slowCloseExceedsWindowThrows() throws Exception {
        FakeSink fake = new FakeSink().blockFor(300L);
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 100L);
        SinkWriter<String> writer = guarded.createWriter(null);

        assertThrows(RuntimeException.class, writer::close,
                "a close that exceeds the stall window must fail the task");
    }

    @Test
    @DisplayName("close bound is capped at CLOSE_BOUND_MS even for a long stall window — "
            + "failure teardown must not delay the task-failure notification past the "
            + "checkpoint timeout (15s flush + 15s close == 30s expiry race)")
    void closeBoundIsCapped() {
        FakeSink fake = new FakeSink();
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 60_000L);

        assertEquals(60_000L, guarded.stallTimeoutMs(), "write/flush keep the full window");
        assertEquals(StallGuardedSink.CLOSE_BOUND_MS, guarded.closeTimeoutMs(),
                "close must be capped so teardown cannot outlast the checkpoint timeout");
    }

    @Test
    @DisplayName("close stall is suppressed when a prior op already stalled — failure teardown "
            + "must not raise a second exception (Flink treats it as a FATAL task-executor "
            + "failure and kills the TaskManager, replacing the stall cause)")
    void closeStallSuppressedAfterPriorStall() throws Exception {
        FakeSink fake = new FakeSink().blockFor(300L);
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 100L);
        SinkWriter<String> writer = guarded.createWriter(null);

        RuntimeException writeStall = assertThrows(RuntimeException.class,
                () -> writer.write("e", null), "the first stall must still throw");
        assertTrue(writeStall.getMessage().contains("write"),
                "the first stall must be the write, got: " + writeStall.getMessage());

        // Teardown close() runs while the writer is already failed: its own stall
        // must be suppressed so the original write-stall stays the task's cause.
        writer.close(); // must NOT throw despite the fake close blocking
    }

    @Test
    @DisplayName("window is per-call, not cumulative: many fast calls never trip the guard")
    void manyFastCallsNeverTrip() throws Exception {
        FakeSink fake = new FakeSink();
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 100L);
        SinkWriter<String> writer = guarded.createWriter(null);

        for (int i = 0; i < 1_000; i++) {
            writer.write("e" + i, null);
        }
        writer.flush(true);
        assertEquals(1_000, fake.writer.written.size());
    }

    @Test
    @DisplayName("pre-write topology forwarded when the delegate supports it (FlussSink does)")
    void preWriteTopologyForwarded() {
        TopologyCountingSink fake = new TopologyCountingSink();
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 1_000L);

        assertNull(guarded.addPreWriteTopology(null),
                "wrapper must return the delegate's pre-write result unchanged");
        assertEquals(1, fake.preWriteCalls,
                "SupportsPreWriteTopology must be delegated to the wrapped sink");
    }

    @Test
    @DisplayName("plain sink without pre-write topology: stream passes through untouched")
    void preWriteTopologyPassesThroughForPlainSink() {
        FakeSink fake = new FakeSink();
        StallGuardedSink<String> guarded = new StallGuardedSink<>(fake, 1_000L);

        assertNull(guarded.addPreWriteTopology(null),
                "a delegate without pre-write topology must leave the stream unchanged");
    }

    @Test
    @DisplayName("wrapper constructor rejects non-positive windows")
    void constructorRejectsNonPositive() {
        FakeSink fake = new FakeSink();
        assertThrows(IllegalArgumentException.class, () -> new StallGuardedSink<>(fake, 0L));
        assertThrows(IllegalArgumentException.class, () -> new StallGuardedSink<>(fake, -5L));
    }

    @Test
    @DisplayName("SINK_WRITE_STALL_TIMEOUT_MS is a governed pin: non-positive config is rejected")
    void configRejectsNonPositivePin() {
        Map<String, String> env = new java.util.HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");

        env.put("SINK_WRITE_STALL_TIMEOUT_MS", "0");
        IllegalStateException zero =
                assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
        assertTrue(zero.getMessage().contains("SINK_WRITE_STALL_TIMEOUT_MS"));

        env.put("SINK_WRITE_STALL_TIMEOUT_MS", "-1");
        IllegalStateException negative =
                assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
        assertTrue(negative.getMessage().contains("SINK_WRITE_STALL_TIMEOUT_MS"));

        env.put("SINK_WRITE_STALL_TIMEOUT_MS", "15000");
        assertEquals(15_000L, SignalJobConfig.from(env).sinkWriteStallTimeoutMs(),
                "explicit 15000 ms must parse and be honored");
    }
}
