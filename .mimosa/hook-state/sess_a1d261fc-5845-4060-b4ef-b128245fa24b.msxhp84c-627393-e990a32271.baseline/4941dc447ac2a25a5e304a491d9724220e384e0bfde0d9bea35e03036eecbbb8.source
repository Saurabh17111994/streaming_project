package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 (throughput redesign): async append semantics.
 *
 * <p>write() returns {@code ACCEPTED} without waiting for the ack; terminal
 * outcomes arrive on the {@link RawTickWriter.OutcomeListener}; the tracker
 * reservation is released exactly once per append, in completion order
 * (which may differ from submission order).
 */
@DisplayName("Phase 2: async append pipelining")
class RawTickWriterAsyncTest {

    /** Stub converter that completes each append asynchronously, out of order. */
    static final class ReorderingConverter implements FlussRowConverter {
        final AtomicInteger appended = new AtomicInteger();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            appended.incrementAndGet();
            // Complete on a fresh async chain so the caller never sees a
            // synchronous completion — later submissions can finish first.
            CompletableFuture<RawTickWriter.AppendResult> f = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> f.complete(
                    new RawTickWriter.AppendResult(42L + appended.get(), "p0")));
            return f;
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 100;
        }

        @Override
        public void close() {}
    }

    @Test
    @DisplayName("1000 async writes: all ACCEPTED, drain → appendCount 1000")
    void asyncWritesAllAcceptedThenDrain() throws Exception {
        ReorderingConverter converter = new ReorderingConverter();
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(30));
        AtomicInteger completions = new AtomicInteger();
        CountDownLatch allDone = new CountDownLatch(1000);
        writer.setOutcomeListener(o -> {
            completions.incrementAndGet();
            allDone.countDown();
        });

        final int N = 1000;
        for (int i = 0; i < N; i++) {
            RawTickWriter.AppendOutcome outcome =
                    writer.write(TickPacketFixtures.validTrade(i));
            assertEquals(RawTickWriter.Status.ACCEPTED, outcome.status());
        }

        // Drain: wait for completion (out of order by construction).
        writer.drain();
        assertTrueAllDone(allDone, N);

        assertEquals(N, completions.get(), "exactly one outcome per write");
        assertEquals(N, writer.appendCount(), "appendCount counts acked rows");
        assertEquals(0, writer.errorCount());
        assertEquals(0, writer.uncertainCount());
        assertEquals(0, tracker.pendingRecords(),
                "every reservation released exactly once");
        assertEquals(0, tracker.pendingBytes());

        writer.close();
    }

    @Test
    @DisplayName("random-order completion releases reservations exactly once each")
    void randomOrderCompletionReleasesOnce() throws Exception {
        // Deterministic shuffle of completion order across 200 appends.
        final int N = 200;
        java.util.List<CompletableFuture<RawTickWriter.AppendResult>> futures =
                Collections.synchronizedList(new ArrayList<>());
        FlussRowConverter delayed = new FlussRowConverter() {
            @Override
            public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
                CompletableFuture<RawTickWriter.AppendResult> f = new CompletableFuture<>();
                futures.add(f);
                return f;
            }

            @Override
            public int estimatedRowSize(TickPacket packet) {
                return 100;
            }

            @Override
            public void close() {}
        };

        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                delayed, tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(30));

        for (int i = 0; i < N; i++) {
            writer.write(TickPacketFixtures.validTrade(i));
        }
        assertEquals(N, tracker.pendingRecords());

        // Complete in reverse submission order (worst case for any
        // completion-ordered accounting bug).
        for (int i = N - 1; i >= 0; i--) {
            futures.get(i).complete(new RawTickWriter.AppendResult(i, "p0"));
        }
        writer.drain();

        assertEquals(N, writer.appendCount());
        assertEquals(0, tracker.pendingRecords(),
                "random-order completion still releases exactly once per row");
        assertEquals(0, tracker.pendingBytes());
        assertEquals(0, writer.errorCount());

        writer.close();
    }

    @Test
    @DisplayName("sync pool-wait throw: write stays ACCEPTED, slot released at terminal (R-297)")
    void syncPoolWaitThrowReleasesSlot() throws Exception {
        // R-297 wedge fix: with a bounded client.writer.buffer.wait-timeout the
        // Fluss client throws SYNCHRONOUSLY (EOFException "Failed to allocate
        // new segment ...") when the memory pool is exhausted. submitAppend
        // must contain it and route through handleCompletion: the reservation
        // made in write() is released exactly at the terminal outcome, and the
        // failure classifies RETRYABLE so retries run on the scheduler thread.
        final int N = 3;
        FlussRowConverter syncThrowing = new FlussRowConverter() {
            @Override
            public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
                // EOFException is checked and the interface throws none; the
                // Fluss client propagates it inside an unchecked wrapper whose
                // cause chain carries the pool-wait message (classifier walks
                // the chain either way).
                throw new IllegalStateException(new java.io.EOFException(
                        "Failed to allocate new segment within the configured max blocking time "
                                + "30000 ms. Total memory: 67108864"));
            }

            @Override
            public int estimatedRowSize(TickPacket packet) {
                return 100;
            }

            @Override
            public void close() {}
        };

        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                syncThrowing, tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(30));
        AtomicInteger terminals = new AtomicInteger();
        CountDownLatch allDone = new CountDownLatch(N);
        writer.setOutcomeListener(o -> {
            terminals.incrementAndGet();
            allDone.countDown();
        });

        for (int i = 0; i < N; i++) {
            RawTickWriter.AppendOutcome outcome =
                    writer.write(TickPacketFixtures.validTrade(i));
            // The sync throw must NOT escape write() — the append is still
            // accepted and its terminal outcome delivered async (after retries).
            assertEquals(RawTickWriter.Status.ACCEPTED, outcome.status());
        }
        assertEquals(N, tracker.pendingRecords(), "reservations held during retries");

        // Retries (backoff 100/200ms) exhaust MAX_RETRY_ATTEMPTS → FAILED.
        writer.drain();
        assertTrueAllDone(allDone, N);

        assertEquals(N, terminals.get(), "one terminal outcome per write");
        assertEquals(N, writer.errorCount(),
                "each sync-throw append ends FAILED after retries exhausted");
        assertEquals(0, tracker.pendingRecords(),
                "sync-throw reservation released at terminal outcome");
        assertEquals(0, tracker.pendingBytes());

        writer.close();
    }

    private static void assertTrueAllDone(CountDownLatch latch, int expected) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("expected " + expected
                        + " outcomes; only " + latch.getCount() + " outstanding after drain");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted awaiting outcomes", e);
        }
    }
}
