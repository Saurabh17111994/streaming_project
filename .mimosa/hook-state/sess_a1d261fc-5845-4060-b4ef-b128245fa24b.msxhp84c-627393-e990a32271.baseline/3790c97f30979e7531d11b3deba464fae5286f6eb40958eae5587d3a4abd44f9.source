package com.trading.compute.signaljob;

import java.util.function.LongSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observability-only source idle-at-tail watchdog (tracker 14 P7/P10,
 * 2026-08-13).
 *
 * <p>A restored Signal job that resumes from a checkpoint at a <em>frozen
 * feed tail</em> consumes ZERO records for as long as the feed is stopped.
 * That is the CORRECT idle-tail state — not a stall (probe-verified
 * 2026-08-13: the "restore stall" was a 3 s time-boxed probe artifact, and
 * zero consumption at the frozen end is exactly what a subscribe-at-end probe
 * shows). This decorator makes that silence observable: when no record has
 * been consumed for {@code SOURCE_IDLE_ALERT_MS} (default 60000), it logs a
 * WARN naming the condition as expected idle-tail behavior and ships a
 * {@code compute.source.idle.at.tail} DELTA metric so OpenObserve can alert —
 * instead of the silence being misread as a hang.
 *
 * <p><b>Graph-safe by construction.</b> This is NOT a new stream operator: it
 * decorates the watermark generator that FLIP-27 runs INSIDE the source
 * operator (one generator per bucket split, created by the
 * {@link CandleWatermarkStrategy} supplier). Adding a mid-stream operator
 * would shift every downstream StreamGraphHasherV2 operator hash and break
 * {@code allowNonRestoredState=false} restore of the P10 archived checkpoints
 * (the KV sink comment documents the same discipline: added LAST to the
 * candles stream on purpose). A watermark-generator decorator adds zero graph
 * nodes, so operator IDs are bit-identical and archived-checkpoint restore is
 * unaffected.
 *
 * <p><b>Why {@code onPeriodicEmit} works as the tick.</b> Flink drives
 * {@code WatermarkGenerator.onPeriodicEmit} on a fixed-delay processing-time
 * timer (auto-watermark-interval, default 200 ms) INDEPENDENT of data flow —
 * verified in flink-runtime 2.2.1 {@code ProgressiveTimestampsAndWatermarks}:
 * {@code startPeriodicWatermarkEmits} schedules the timer and
 * {@code SourceOutputWithWatermarks.emitPeriodicWatermark} invokes
 * {@code onPeriodicEmit} even when the source produces nothing. So a frozen
 * tail still ticks the watchdog; {@code onEvent} only stamps the last-record
 * wall clock.
 *
 * <p><b>Episode semantics.</b> One WARN + one metric delta per idle EPISODE
 * job-wide, not per periodic tick and not per split: the first generator
 * (any bucket) to cross the threshold reports once via a static episode latch;
 * the latch clears when ANY record arrives (any split), so a resumed feed
 * logs INFO and the next idle episode reports again. The latch is a JVM-wide
 * static — exact for the embedded dev run (single process, parallelism 1),
 * the documented scope of the other ComputeOtlpEmitter statics.
 *
 * <p>Healthy-path behavior is a pure pass-through: {@code onEvent} delegates
 * the bounded-out-of-orderness watermark emission unchanged; the wrapper adds
 * only a wall-clock stamp. No offsets are touched, no connector patch, no
 * defensive clamp — observability only.
 */
final class SourceIdleWatchdogGenerator implements WatermarkGenerator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceIdleWatchdogGenerator.class);

    /**
     * Job-level episode latch: true while an idle episode has been reported.
     * Cleared by the first {@code onEvent} from any split (any generator
     * instance), so a resumed feed re-arms the next episode. JVM-wide static:
     * exact for the single-process embedded dev run (parallelism 1); a
     * distributed run would report per TaskManager — documented limitation,
     * same as the emitter statics.
     */
    private static final java.util.concurrent.atomic.AtomicBoolean EPISODE_REPORTED =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    private final WatermarkGenerator<RowData> delegate;
    private final long sourceIdleAlertMs;
    private final LongSupplier clock;
    private long lastEventWallClockMs;

    SourceIdleWatchdogGenerator(WatermarkGenerator<RowData> delegate, long sourceIdleAlertMs) {
        this(delegate, sourceIdleAlertMs, System::currentTimeMillis);
    }

    /** Clock-injectable constructor (tests); the wall clock is used for idle duration only. */
    SourceIdleWatchdogGenerator(
            WatermarkGenerator<RowData> delegate, long sourceIdleAlertMs, LongSupplier clock) {
        this.delegate = delegate;
        this.sourceIdleAlertMs = sourceIdleAlertMs;
        this.clock = clock;
        // Start the idle clock at construction: a restored source at a frozen
        // tail has no events, so the first periodic tick after the threshold
        // correctly measures "idle since source start".
        this.lastEventWallClockMs = clock.getAsLong();
    }

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
        // REQ-FC-010 source throughput: covered natively by the FLIP-27 source
        // operator's numRecordsOut / numRecordsOutPerSecond metrics (CHG-023
        // item 1 removed the client-side ComputeOtlpEmitter mirror).
        delegate.onEvent(event, eventTimestamp, output);
        long now = clock.getAsLong();
        if (EPISODE_REPORTED.getAndSet(false)) {
            LOG.info("signal-job: source resumed after {} ms idle at the tail — records flowing "
                    + "again (feed resumed or restore caught up)", now - lastEventWallClockMs);
        }
        lastEventWallClockMs = now;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // REQ-FC-010 watermark lag: covered natively by the source operator's
        // currentOutputWatermark gauge (lag = now - watermark at scrape time)
        // — CHG-023 item 1 removed the client-side ComputeOtlpEmitter gauge.
        delegate.onPeriodicEmit(output);
        long now = clock.getAsLong();
        long idleMs = now - lastEventWallClockMs;
        // Edge-triggered: report once per idle episode (compareAndSet wins for
        // exactly one generator even if several splits cross together).
        if (idleMs >= sourceIdleAlertMs && EPISODE_REPORTED.compareAndSet(false, true)) {
            LOG.warn("signal-job: source idle at the tail — no records consumed for {} ms "
                    + "(>= SOURCE_IDLE_ALERT_MS={} ms). This is EXPECTED idle-tail behavior when "
                    + "the feed is stopped or a restored source sits at a frozen log end (NOT a "
                    + "stall; probe-verified 2026-08-13 — verify with a raw-scanner probe "
                    + "subscribing at the tail). Investigate only if the feed should be live.",
                    idleMs, sourceIdleAlertMs);
            // The old compute.source.idle.at.tail DELTA metric was removed with
            // the emitter (CHG-023 item 1): the native idle signal is the source
            // operator's numRecordsOutPerSecond meter dropping to 0 + the
            // currentOutputWatermark gauge freezing (O2 alert retargets to
            // those native series). The WARN/INFO episode logs remain the
            // primary operator-facing signal.
        }
    }

    /** TEST-ONLY: resets the job-wide episode latch (keeps tests independent). */
    static void resetEpisodeForTest() {
        EPISODE_REPORTED.set(false);
    }

    /** TEST-ONLY: current episode-latch state (package-visible for tests). */
    static boolean episodeReportedForTest() {
        return EPISODE_REPORTED.get();
    }
}
