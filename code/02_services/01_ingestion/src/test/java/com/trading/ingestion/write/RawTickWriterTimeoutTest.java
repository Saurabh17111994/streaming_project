package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-037 — on timeout the in-flight append is cancelled and the backpressure
 * release is deferred to the append future's completion (never at the
 * {@code get()} timeout while the append may still be running in the Fluss
 * client's background).
 *
 * <p>Regression: the previous code called {@code tracker.onAppendFailure()}
 * synchronously at the timeout while abandoning the future, so the AppendTracker
 * contract "pending counters decrease only after append completes" was violated
 * and in-flight work was under-counted under sustained Fluss latency.
 */
@DisplayName("R-037: RawTickWriter timeout accounting")
class RawTickWriterTimeoutTest {

    /** Stub converter whose append future is controlled by the test. */
    static final class ControllableConverter implements FlussRowConverter {
        volatile CompletableFuture<RawTickWriter.AppendResult> pending =
                new CompletableFuture<>();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            return pending;
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 100;
        }

        @Override
        public void close() {}
    }

    @Test
    @DisplayName("timeout cancels the in-flight append and releases via the future")
    void timeoutCancelsAndReleasesViaFuture() {
        ControllableConverter converter = new ControllableConverter();
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, "default.raw_table_1",
                Duration.ofMillis(50), Duration.ofSeconds(1));

        RawTickWriter.AppendOutcome outcome = writer.write(TickPacketFixtures.validTrade(0));

        assertEquals(RawTickWriter.Status.UNCERTAIN, outcome.status(),
                "timeout outcome stays UNCERTAIN (may be a duplicate)");
        assertTrue(converter.pending.isCancelled(),
                "in-flight append must be cancelled on timeout");

        // The reservation is released by the future's completion (cancellation),
        // not while the append might still be running — no leaked reservation,
        // and the tracker contract holds.
        assertEquals(0, tracker.pendingRecords(),
                "no leaked reservation after timeout");
        assertEquals(0, tracker.pendingBytes());
    }

    @Test
    @DisplayName("late completion of a timed-out append does not double-release")
    void lateCompletionDoesNotDoubleRelease() {
        ControllableConverter converter = new ControllableConverter();
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, "default.raw_table_1",
                Duration.ofMillis(50), Duration.ofSeconds(1));

        writer.write(TickPacketFixtures.validTrade(1));
        assertEquals(0, tracker.pendingRecords());

        // Simulate the abandoned append surfacing a late failure in the
        // client's background — the accounting must stay exact (no double
        // release below zero, no second reservation).
        converter.pending.completeExceptionally(new RuntimeException("late failure"));
        assertEquals(0, tracker.pendingRecords(),
                "late completion must not re-release the reservation");
        assertEquals(0, tracker.pendingBytes());
    }

    @Test
    @DisplayName("successful appends still release once")
    void successfulAppendReleasesOnce() {
        ControllableConverter converter = new ControllableConverter();
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(1));

        // Complete the append before write() so the ack path is exercised.
        converter.pending.complete(new RawTickWriter.AppendResult(42L, "p0"));
        RawTickWriter.AppendOutcome outcome = writer.write(TickPacketFixtures.validTrade(2));

        assertEquals(RawTickWriter.Status.SUCCESS, outcome.status());
        assertEquals(0, tracker.pendingRecords(),
                "success releases the reservation exactly once");
        assertEquals(1, writer.appendCount());
    }
}
