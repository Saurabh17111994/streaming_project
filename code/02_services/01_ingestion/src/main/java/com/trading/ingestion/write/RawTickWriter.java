package com.trading.ingestion.write;

import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded Fluss append writer for {@code raw_table_1}.
 *
 * <p>Contract:
 * <ul>
 *   <li>No application batching: each tick is submitted individually
 *       ({@code INGESTION_MAX_BATCH_RECORDS=1, INGESTION_MAX_BATCH_WAIT_MS=0})</li>
 *   <li>Backpressure: before every append, calls {@link AppendTracker#tryAccept(int)}
 *       with the row size estimate; rejects if halted</li>
 *   <li>Every append records receive-time, append-start, append-acknowledgement
 *       time, append outcome, record size, and error class</li>
 *   <li>Pending counters decrease only after append completes (success or fail)</li>
 *   <li>Arrow payloads are never compressed in the ingestion→Fluss path</li>
 *   <li>Raw ingestion does not deduplicate fingerprints; Compute owns logical dedup</li>
 *   <li>Retry with linear backoff (up to {@code MAX_RETRY_ATTEMPTS}) for RETRYABLE
 *       failures; FATAL failures halt immediately</li>
 *   <li>On timeout the outcome is {@code UNCERTAIN} — ingestion cannot prove
 *       whether Fluss persisted the row; Compute owns logical dedup</li>
 * </ul>
 *
 * <p>This class wraps the Fluss client table writer. The concrete Fluss
 * append API is version-gated on Fluss {@code 0.9.1-incubating}.
 */
public final class RawTickWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RawTickWriter.class);

    /** Maximum retry attempts for RETRYABLE failures before giving up. */
    static final int MAX_RETRY_ATTEMPTS = 3;
    /** Initial backoff delay between retries (ms). Doubles each attempt. */
    static final long BASE_RETRY_BACKOFF_MS = 100;

    private final FlussRowConverter rowConverter;
    private final AppendTracker tracker;
    private final Duration appendTimeout;
    private final Duration drainDeadline;
    private final String tableName;
    private final AtomicLong appendCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong uncertainCount = new AtomicLong(0);
    private volatile boolean closed;

    /**
     * @param rowConverter  converts {@link TickPacket} → Fluss row
     * @param tracker       shared backpressure tracker
     * @param tableName     for logging/observability
     * @param appendTimeout per-append deadline
     * @param drainDeadline max time to wait for pending writes on shutdown
     */
    public RawTickWriter(FlussRowConverter rowConverter,
                         AppendTracker tracker,
                         String tableName,
                         Duration appendTimeout,
                         Duration drainDeadline) {
        this.rowConverter = rowConverter;
        this.tracker = tracker;
        this.tableName = tableName;
        this.appendTimeout = appendTimeout;
        this.drainDeadline = drainDeadline;
    }

    /**
     * Convert a tick packet to a Fluss row, reserve backpressure capacity,
     * and submit the append individually.
     *
     * <p>Delivery is at-least-once: every accepted tick is appended exactly
     * once; ingestion never deduplicates by fingerprint (logical dedup belongs
     * to the Signal Flink job, plan §Executive Summary).
     *
     * <p>Retry: on RETRYABLE failures (per {@link RetryClassifier}) the writer
     * retries up to {@link #MAX_RETRY_ATTEMPTS} times with linear backoff.
     * Timeout outcomes are classified {@code UNCERTAIN} — the append may have
     * succeeded at Fluss but the ack was lost.
     *
     * @param packet the decoded+normalized+fingerprinted tick
     * @return outcome with timing
     */
    public AppendOutcome write(TickPacket packet) {
        if (closed) {
            errorCount.incrementAndGet();
            return AppendOutcome.skipped("writer closed");
        }

        String fp = packet.eventFingerprint();

        // 1. Estimate row size for backpressure
        int rowBytes = rowConverter.estimatedRowSize(packet);

        // 2. Reserve backpressure capacity
        if (!tracker.tryAccept(rowBytes)) {
            return AppendOutcome.rejected(
                    tracker.pendingRecords(), tracker.pendingBytes(),
                    "pending-limit exceeded; halted=" + tracker.isHalted());
        }

        // 3. Record ingestion timestamp
        Instant acceptTime = Instant.now();

        // 4. Submit with retry loop
        int attempt = 0;
        CompletableFuture<AppendResult> future = null;
        while (attempt < MAX_RETRY_ATTEMPTS) {
            attempt++;
            try {
                // Submit append (no batching — one tick, one call)
                future = rowConverter.append(packet);

                // Wait for acknowledgement
                AppendResult result = future.get(appendTimeout.toMillis(), TimeUnit.MILLISECONDS);

                // ---- Success ----
                tracker.onAppendSuccess(rowBytes);
                appendCount.incrementAndGet();
                return AppendOutcome.success(
                        packet.eventTime(), acceptTime, Instant.now(),
                        rowBytes, result);

            } catch (TimeoutException e) {
                // Timeout → UNCERTAIN: we don't know if Fluss persisted the row.
                // Do NOT retry — the same row could already be durably stored.
                // Compute owns logical dedup at the Flink level.
                //
                // R-037: cancel the in-flight append and defer the backpressure
                // release until the future actually completes. The AppendTracker
                // contract is "pending counters decrease only after append
                // completes" — releasing now would under-count in-flight work
                // while the append keeps running in the client's background.
                if (future != null) {
                    future.cancel(true);
                    future.whenComplete((r, ex) -> tracker.onAppendFailure(rowBytes));
                } else {
                    tracker.onAppendFailure(rowBytes);
                }
                errorCount.incrementAndGet();
                uncertainCount.incrementAndGet();
                LOG.warn("raw-writer: append UNCERTAIN (table={}, fp={}, timeout={}ms, attempt={})",
                        tableName,
                        fp != null ? fp.substring(0, Math.min(12, fp.length())) : "null",
                        appendTimeout.toMillis(), attempt);
                return AppendOutcome.uncertain(rowBytes, appendTimeout);

            } catch (Exception e) {
                RetryClassifier.Classification retry = RetryClassifier.classify(e);

                if (retry == RetryClassifier.Classification.FATAL) {
                    // Fatal → no retry, halt the append path
                    tracker.onAppendFailure(rowBytes);
                    errorCount.incrementAndGet();
                    LOG.error("raw-writer: FATAL append error (table={}, class={})",
                            tableName, e.getClass().getSimpleName());
                    return AppendOutcome.fatal(rowBytes, e);
                }

                // RETRYABLE — retry with backoff if attempts remain
                if (attempt < MAX_RETRY_ATTEMPTS) {
                    long backoffMs = BASE_RETRY_BACKOFF_MS * (1L << (attempt - 1));
                    LOG.warn("raw-writer: append retryable (table={}, attempt={}/{}, backoff={}ms, class={})",
                            tableName, attempt, MAX_RETRY_ATTEMPTS, backoffMs,
                            e.getClass().getSimpleName());
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        tracker.onAppendFailure(rowBytes);
                        errorCount.incrementAndGet();
                        return AppendOutcome.failed(rowBytes, new RuntimeException(
                                "Retry interrupted after " + attempt + " attempts", e));
                    }
                } else {
                    // Exhausted retries
                    tracker.onAppendFailure(rowBytes);
                    errorCount.incrementAndGet();
                    LOG.warn("raw-writer: append failed after {} attempts (table={})",
                            MAX_RETRY_ATTEMPTS, tableName, e);
                    return AppendOutcome.failed(rowBytes, e);
                }
            }
        }

        // Should be unreachable — the loop always returns or breaks.
        tracker.onAppendFailure(rowBytes);
        errorCount.incrementAndGet();
        return AppendOutcome.failed(rowBytes,
                new IllegalStateException("Exhausted retry attempts"));
    }


    public long appendCount() { return appendCount.get(); }
    public long errorCount() { return errorCount.get(); }
    public long uncertainCount() { return uncertainCount.get(); }

    /**
     * Drain pending writes and close. Waits up to {@code drainDeadline}
     * for pending records to reach zero before force-closing the connection.
     */
    @Override
    public void close() {
        closed = true;
        long deadlineNanos = System.nanoTime() + drainDeadline.toNanos();
        long pendingAtStart = tracker.pendingRecords();

        // Drain: wait for pending appends to complete
        while (tracker.pendingRecords() > 0
                && System.nanoTime() < deadlineNanos) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        long remaining = tracker.pendingRecords();
        if (remaining > 0) {
            LOG.warn("raw-writer: drain incomplete — {} records still pending "
                    + "(deadline={}, started={})",
                    remaining, drainDeadline, pendingAtStart);
            tracker.onAppendFailure((int) (remaining * 512)); // approximate
        }

        LOG.info("raw-writer: closed (table={}, appended={}, errors={}, "
                + "uncertain={}, "
                + "pending_at_start={}, pending_remaining={})",
                tableName, appendCount.get(), errorCount.get(),
                uncertainCount.get(),
                pendingAtStart, remaining);
    }

    // ---- outcome type ----

    public enum Status { SUCCESS, UNCERTAIN, TIMEOUT, FAILED, FATAL, REJECTED, SKIPPED }

    public record AppendOutcome(
            Status status,
            Instant eventTime,
            Instant acceptTime,
            Instant ackTime,
            int rowBytes,
            String detail,
            long pendingRecords,
            long pendingBytes
    ) {
        static AppendOutcome success(Instant eventTime, Instant acceptTime,
                                     Instant ackTime, int rowBytes, AppendResult result) {
            return new AppendOutcome(Status.SUCCESS, eventTime, acceptTime, ackTime,
                    rowBytes, result.toString(), -1, -1);
        }

        /** Append timed out — Fluss may have persisted the row. */
        static AppendOutcome uncertain(int rowBytes, Duration timeout) {
            return new AppendOutcome(Status.UNCERTAIN, null, null, null, rowBytes,
                    "uncertain after " + timeout.toMillis() + "ms timeout; may be a duplicate",
                    -1, -1);
        }

        static AppendOutcome timeout(int rowBytes, Duration timeout) {
            return new AppendOutcome(Status.TIMEOUT, null, null, null, rowBytes,
                    "timeout after " + timeout.toMillis() + "ms", -1, -1);
        }

        static AppendOutcome fatal(int rowBytes, Exception e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getClass().getSimpleName();
            return new AppendOutcome(Status.FATAL, null, null, null, rowBytes,
                    "FATAL: " + msg, -1, -1);
        }

        static AppendOutcome failed(int rowBytes, Exception e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getClass().getSimpleName();
            return new AppendOutcome(Status.FAILED, null, null, null, rowBytes,
                    msg, -1, -1);
        }

        static AppendOutcome rejected(long pendingRecs, long pendingBytes, String detail) {
            return new AppendOutcome(Status.REJECTED, null, null, null, 0,
                    detail, pendingRecs, pendingBytes);
        }

        static AppendOutcome skipped(String detail) {
            return new AppendOutcome(Status.SKIPPED, null, null, null, 0, detail, -1, -1);
        }
    }

    /** Minimal result from Fluss append acknowledgement. */
    public record AppendResult(long offset, String partition) {
        @Override
        public String toString() {
            return "offset=" + offset + ", partition=" + partition;
        }
    }
}
