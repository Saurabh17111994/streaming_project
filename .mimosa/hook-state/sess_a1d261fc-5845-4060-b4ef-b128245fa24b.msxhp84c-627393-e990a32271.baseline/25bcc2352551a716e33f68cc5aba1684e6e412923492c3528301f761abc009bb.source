package com.trading.compute.signaljob;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink-side sink-write stall guard (tracker 14 box 682/116, 2026-08-12).
 *
 * <p>Bounded-failure watchdog for the Fluss candle sinks. A mid-run sink
 * failure (e.g. the KV table deleted) must take the whole job to terminal
 * FAILED deterministically; the raw Fluss client does not do that on its own:
 * <ul>
 *   <li>{@code flush()} blocks in {@code RecordAccumulator.awaitFlushCompletion()}
 *       (a {@code CountDownLatch}) that never counts down while the deleted
 *       table's batch stays undrained, and</li>
 *   <li>{@code close()} blocks in {@code ExecutorService.awaitTermination(Long.MAX_VALUE)}
 *       because the sender's shutdown drain loop
 *       ({@code while (!forceClose && hasUnDrained()) runOnce()}) never exits —
 *       {@code forceClose()} is only invoked AFTER {@code awaitTermination}
 *       returns, a circular deadlock in the client (bytecode-verified in
 *       fluss-client-0.9.1-incubating: {@code WriterClient.close(Duration)}).</li>
 * </ul>
 * Neither is reachable by configuration: the deleted table's batch never
 * fails (metadata update for the dropped table is swallowed, {@code readyNodes}
 * stays empty), so {@code client.writer.retries} is never consulted.
 *
 * <p>This wrapper therefore bounds every delegate call (write, flush,
 * writeWatermark, close) itself: each call runs on a dedicated single-thread
 * executor and the caller waits at most {@code stallTimeoutMs} — except
 * {@code close()}, which is capped at {@link #CLOSE_BOUND_MS} (5 s) so a
 * stalled close during failure teardown cannot delay the task-failure
 * notification past the checkpoint timeout. On timeout the worker is
 * interrupted (both Fluss hang points convert
 * {@link InterruptedException} into a fast exit — {@code flush()} throws, and
 * {@code close()}'s interrupt handler runs {@code shutdownNow()} +
 * {@code forceClose()}) and a {@link RuntimeException} naming the stalled
 * operation is thrown, failing the task so the configured restart policy
 * drives the job to terminal FAILED. A short unwind grace is given so the
 * client can drain its sender thread before the exception is raised.
 *
 * <p>Healthy-path writes complete in milliseconds, so the pinned 15000 ms
 * default is ~10x headroom. Shared-fate semantics are preserved: both candle
 * sinks (LOG + KV) get an identical guard, so deleting either table still
 * takes the WHOLE job down — now to FAILED, not a hang.
 *
 * <p>Also forwards {@link SupportsPreWriteTopology} when the delegate is one
 * (FlussSink is) so the per-bucket pre-write shuffle is preserved; a plain
 * delegate keeps the stream unchanged.
 *
 * <p>Note on the Flink 2.2.1 sink2 surface (verified via javap of
 * flink-core-2.2.1): {@code SinkWriter} no longer carries
 * prepareCommit/snapshotState — the two-phase commit lives in optional
 * {@code CommittingSinkWriter}/{@code StatefulSinkWriter} interfaces that the
 * Fluss sink does not implement — so the guard covers exactly the live write
 * path: write / flush / writeWatermark / close.
 *
 * @param <InputT> element type
 */
public final class StallGuardedSink<InputT> implements Sink<InputT>, SupportsPreWriteTopology<InputT> {

    private static final Logger LOG = LoggerFactory.getLogger(StallGuardedSink.class);
    private static final long serialVersionUID = 1L;

    /** How long to wait for the interrupted worker to unwind before raising the stall. */
    private static final long STALL_UNWIND_GRACE_MS = 2_000L;

    /**
     * Hard cap for the {@code close()} bound. Deliberately much shorter than the
     * write/flush bound: a stalled close happens during FAILURE TEARDOWN (the
     * task's {@code cleanUp} runs {@code SinkWriterOperator.close} after the
     * flush-stall has already failed the mailbox loop), so a 15 s close stall
     * would delay the task-failure notification to the JobMaster by another
     * 15 s — 15 s flush + 15 s close = the 30 s checkpoint timeout, letting the
     * coordinator's checkpoint expiry win the race and fail the job with
     * "Exceeded checkpoint tolerable failure threshold" instead of the stall
     * cause (observed in the 2026-08-12 kv-drop gated run). A healthy close is
     * milliseconds and the interrupted Fluss close unwinds in ~5 ms, so 5 s is
     * ~1000x headroom while keeping the failure notification ~10 s ahead of
     * the checkpoint expiry.
     */
    static final long CLOSE_BOUND_MS = 5_000L;

    private final Sink<InputT> delegate;
    private final long stallTimeoutMs;
    private final long closeTimeoutMs;

    public StallGuardedSink(Sink<InputT> delegate, long stallTimeoutMs) {
        if (stallTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "stall timeout must be > 0 ms, got " + stallTimeoutMs);
        }
        this.delegate = delegate;
        this.stallTimeoutMs = stallTimeoutMs;
        this.closeTimeoutMs = Math.min(stallTimeoutMs, CLOSE_BOUND_MS);
    }

    /** The bound applied to every writer call (ms); package-visible for tests. */
    long stallTimeoutMs() {
        return stallTimeoutMs;
    }

    /** The (capped) bound applied to {@code close()} (ms); package-visible for tests. */
    long closeTimeoutMs() {
        return closeTimeoutMs;
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new StallGuardedWriter<>(delegate.createWriter(context), stallTimeoutMs,
                closeTimeoutMs);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
        if (delegate instanceof SupportsPreWriteTopology) {
            return ((SupportsPreWriteTopology<InputT>) delegate).addPreWriteTopology(input);
        }
        return input;
    }

    /**
     * Delegating writer that runs every delegate call on a single worker
     * thread with a hard deadline; on timeout the worker is interrupted and a
     * stall {@link RuntimeException} is thrown. A single worker thread keeps
     * delegate calls strictly ordered (the Fluss writer is not thread-safe).
     */
    static final class StallGuardedWriter<InputT> implements SinkWriter<InputT> {

        private final SinkWriter<InputT> delegate;
        private final long stallTimeoutMs;
        private final long closeTimeoutMs;
        private final ExecutorService delegateExecutor;

        /**
         * Set when ANY guarded call times out. The failure teardown path then
         * suppresses a second (close) stall so the ORIGINAL stall stays the
         * task's failure cause — a close-stall raised while the task is already
         * failing becomes Flink's "FATAL - exception in exception handler" and
         * kills the whole TaskExecutor ("TaskManager #0 failed", job cause
         * "The TaskExecutor is shutting down"), observed in the 2026-08-12
         * kv-drop gated runs.
         */
        private volatile boolean stalled;

        StallGuardedWriter(SinkWriter<InputT> delegate, long stallTimeoutMs, long closeTimeoutMs) {
            this.delegate = delegate;
            this.stallTimeoutMs = stallTimeoutMs;
            this.closeTimeoutMs = closeTimeoutMs;
            this.delegateExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "stall-guard-writer");
                t.setDaemon(true);
                return t;
            });
        }

        @Override
        public void write(InputT element, Context context) throws IOException, InterruptedException {
            runBounded("write", stallTimeoutMs, () -> {
                delegate.write(element, context);
                return null;
            });
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            runBounded("flush", stallTimeoutMs, () -> {
                delegate.flush(endOfInput);
                return null;
            });
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            runBounded("writeWatermark", stallTimeoutMs, () -> {
                delegate.writeWatermark(watermark);
                return null;
            });
        }

        @Override
        public void close() throws Exception {
            boolean alreadyStalled = stalled;
            try {
                // Capped bound (see CLOSE_BOUND_MS): a stalled close during
                // failure teardown must not delay the task-failure notification
                // past the checkpoint timeout.
                runBounded("close", closeTimeoutMs, () -> {
                    delegate.close();
                    return null;
                });
            } catch (RuntimeException e) {
                if (alreadyStalled) {
                    // Failure teardown (cleanUp after a stall has already failed
                    // the mailbox loop): re-raising would become Flink's FATAL
                    // "exception in exception handler" and kill the whole
                    // TaskExecutor, replacing the stall cause with "The
                    // TaskExecutor is shutting down". The original stall is the
                    // actionable failure — suppress the teardown close-stall.
                    LOG.warn("sink write-path stall: close stalled while the writer was "
                            + "already failed — suppressing (the original stall is the "
                            + "actionable failure, tracker 14 box 682/116)");
                } else {
                    throw e;
                }
            } finally {
                delegateExecutor.shutdownNow();
            }
        }

        /**
         * Runs a delegate call with a hard {@code boundMs} deadline. The
         * delegate executes on the worker thread; the caller blocks in
         * {@code Future.get}. On timeout the worker is interrupted (the Fluss
         * client converts that into a fast exit on both its hang points), a
         * short unwind grace lets the sender thread drain, then a stall
         * {@link RuntimeException} is thrown to fail the task. Delegate
         * exceptions are unwrapped and rethrown as-is.
         */
        private <T> T runBounded(String op, long boundMs, Callable<T> call)
                throws IOException, InterruptedException {
            Future<T> future = delegateExecutor.submit(call);
            try {
                return future.get(boundMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                stalled = true;
                future.cancel(true); // interrupt the blocked delegate call
                try {
                    // A cancelled Future's get() throws CancellationException
                    // IMMEDIATELY (it never waits and never times out), so the
                    // cancelled worker future cannot serve as the unwind wait —
                    // before this sentinel, the CancellationException (null
                    // message) escaped runBounded and Flink treated it as a
                    // benign checkpoint cancel, silently turning the stall into
                    // a 30 s checkpoint expiry. Queue a sentinel behind the
                    // worker instead: on the single-thread executor it runs
                    // only once the (possibly interrupted) delegate call has
                    // actually returned, so this really bounds the unwind.
                    delegateExecutor.submit(() -> {
                        // sentinel: completes when the stalled call exits
                    }).get(STALL_UNWIND_GRACE_MS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException graceTimedOut) {
                    LOG.warn("sink write-path stall: {} did not unwind within {} ms after "
                            + "interrupt — abandoning the worker (daemon thread)", op,
                            STALL_UNWIND_GRACE_MS);
                } catch (ExecutionException | CancellationException | InterruptedException ignored) {
                    // worker unwound; the stall below is the actionable failure
                }
                LOG.error("sink write-path stall: {} did not complete within {} ms — "
                        + "failing the task (tracker 14 box 682/116)", op, boundMs);
                throw new RuntimeException("sink write-path stall: " + op + " exceeded "
                        + boundMs + " ms");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                if (cause instanceof InterruptedException) {
                    throw (InterruptedException) cause;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Exception) {
                    throw new IOException("delegate " + op + " failed", cause);
                }
                throw new RuntimeException("delegate " + op + " failed", cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }
}
