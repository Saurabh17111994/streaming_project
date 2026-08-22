package com.trading.ingestion.write;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded pending-append counters — records and bytes.
 *
 * <p>Thread-safe. Enforces the backpressure contract from
 * {@code docs/04_contracts/01-ingestion.md} and streaming-3000 plan T2:
 *
 * <ul>
 *   <li>Max pending records: 150,000 default (tunable 50k/64M for 1k → 150k/192M for 3k)
 *       via {@code MAX_PENDING_APPEND_RECORDS} / alias {@code PENDING_MAX_RECORDS}</li>
 *   <li>Max pending bytes: 192 MiB default (tunable) via
 *       {@code MAX_PENDING_APPEND_BYTES} / alias {@code PENDING_MAX_BYTES}</li>
 *   <li>80% warning (tunable via {@code PENDING_APPEND_WARNING_PERCENT} /
 *       alias {@code PENDING_WARNING_PERCENT}) → readiness false, warning event emitted</li>
 *   <li>100% → stop accepting broker data, readiness false, critical event,
 *       preserved acknowledged-loss record; never silently discard</li>
 *   <li>Pending counters decrease only after append completes</li>
 *   <li>Halt is fail-closed until process restart (ING-FAIL-005)</li>
 * </ul>
 *
 * <p>Thresholds are env-driven: 80% warn / 100% halt. Defaults raised for 3k
 * scale (streaming-3000 G2 Ingest T2). Keep halt logic — never drop silently.
 *
 * <p>Events are delivered through a pluggable {@link BackpressureListener}
 * so the tracker remains independent of logging/telemetry.
 */
public final class AppendTracker {

    // T2 tunable backpressure (G2 Ingest) — streaming-3000: 50k/64M → 150k/192M
    public static final long MAX_PENDING_RECORDS = 150_000L;
    static final long MAX_PENDING_BYTES = 201_326_592L; // 192 MiB
    static final double WARNING_PERCENT = 0.80;

    private final AtomicLong pendingRecords = new AtomicLong(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);

    private final AtomicLong totalAccepted = new AtomicLong(0);
    private final AtomicLong totalAppended = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private final AtomicLong totalRejected = new AtomicLong(0);
    private final AtomicLong totalBytesAccepted = new AtomicLong(0);

    private volatile boolean halted;
    private volatile Instant lastWarningAt;
    private final AtomicLong warningCount = new AtomicLong(0);
    private final long maxPendingRecords;
    private final long maxPendingBytes;
    private final double warningPercent;

    private volatile BackpressureListener listener = BackpressureListener.NOOP;

    public AppendTracker() {
        this(MAX_PENDING_RECORDS, MAX_PENDING_BYTES, WARNING_PERCENT);
    }

    public AppendTracker(long maxPendingRecords, long maxPendingBytes, double warningPercent) {
        if (maxPendingRecords <= 0 || maxPendingBytes <= 0 || warningPercent <= 0 || warningPercent >= 1) {
            throw new IllegalArgumentException("invalid pending limits");
        }
        this.maxPendingRecords = maxPendingRecords;
        this.maxPendingBytes = maxPendingBytes;
        this.warningPercent = warningPercent;
    }

    // ---- listener ----

    @FunctionalInterface
    public interface BackpressureListener {
        void onEvent(Level level, long pendingRecords, long pendingBytes,
                     long maxRecords, long maxBytes, Instant now);
        enum Level { WARNING, CRITICAL }
        BackpressureListener NOOP = (l, pr, pb, mr, mb, n) -> {};
    }

    public void setListener(BackpressureListener l) {
        this.listener = l != null ? l : BackpressureListener.NOOP;
    }

    // ---- accept gate ----

    /**
     * Reserve capacity for one record. Returns {@code true} if the record
     * can be accepted; {@code false} if it would exceed a limit (halted).
     * Caller MUST NOT submit the record on false — it was not counted.
     */
    public boolean tryAccept(int recordBytes) {
        // R-195: check-then-act on `halted` was racy — a thread could pass the
        // check and then increment counters after a concurrent halt. Serialize
        // the accept gate with the halt transition.
        synchronized (this) {
            if (halted) {
                totalRejected.incrementAndGet();
                return false;
            }

            long recs = pendingRecords.incrementAndGet();
            long byt = pendingBytes.addAndGet(recordBytes);

            // 100% halt — immediate, no negotiation
            if (recs > maxPendingRecords || byt > maxPendingBytes) {
                halted = true;
                pendingRecords.decrementAndGet();
                pendingBytes.addAndGet(-recordBytes);
                totalRejected.incrementAndGet();
                listener.onEvent(BackpressureListener.Level.CRITICAL,
                        recs, byt, maxPendingRecords, maxPendingBytes, Instant.now());
                return false;
            }

            // R-196: counters and totals must be consistent BEFORE the
            // warning listener fires — it reads pending + totals.
            totalAccepted.incrementAndGet();
            totalBytesAccepted.addAndGet(recordBytes);

            // 80% warning — still accept, but flag readiness
            double recPct = (double) recs / maxPendingRecords;
            double bytPct = (double) byt / maxPendingBytes;
            if (recPct >= warningPercent || bytPct >= warningPercent) {
                warningCount.incrementAndGet();
                Instant now = Instant.now();
                // throttle: emit warning at most once per 30 s
                if (lastWarningAt == null || lastWarningAt.plusSeconds(30).isBefore(now)) {
                    lastWarningAt = now;
                    listener.onEvent(BackpressureListener.Level.WARNING,
                            recs, byt, maxPendingRecords, maxPendingBytes, now);
                }
            }
            return true;
        }
    }

    /** MUST be called once per accepted record after Fluss acknowledges the append. */
    public void onAppendSuccess(int recordBytes) {
        pendingRecords.decrementAndGet();
        pendingBytes.addAndGet(-recordBytes);
        totalAppended.incrementAndGet();
    }

    /** MUST be called once per accepted record when Fluss append fails. */
    public void onAppendFailure(int recordBytes) {
        pendingRecords.decrementAndGet();
        pendingBytes.addAndGet(-recordBytes);
        totalFailed.incrementAndGet();
    }

    // ---- health ----

    public boolean isHalted() { return halted; }

    public long pendingRecords() { return pendingRecords.get(); }
    public long pendingBytes() { return pendingBytes.get(); }
    /** The configured max pending records (R-109: consumers must use this, not a static). */
    public long maxPendingRecords() { return maxPendingRecords; }
    public long maxPendingBytes() { return maxPendingBytes; }
    public long totalAccepted() { return totalAccepted.get(); }
    public long totalAppended() { return totalAppended.get(); }
    public long totalFailed() { return totalFailed.get(); }
    public long totalRejected() { return totalRejected.get(); }
    public long totalBytesAccepted() { return totalBytesAccepted.get(); }
    public long warningCount() { return warningCount.get(); }

    /**
     * Readiness: not-halted AND below warning threshold on both axes.
     * Per dossier: readiness false at ≥80% of either pending limit.
     */
    public boolean isReady() {
        if (halted) return false;
        double recPct = (double) pendingRecords.get() / maxPendingRecords;
        double bytPct = (double) pendingBytes.get() / maxPendingBytes;
        return recPct < warningPercent && bytPct < warningPercent;
    }
}
