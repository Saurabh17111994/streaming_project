package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SourceIdleWatchdogGenerator behavior with an injectable clock (tracker 14
 * P7/P10 — 2026-08-13 misdiagnosis lesson): the watchdog is observability
 * ONLY — it must never alter watermark emission (pure pass-through), must log
 * ONE alert per idle EPISODE (not per periodic tick), and must re-arm after
 * records resume.
 *
 * <p>CHG-023 item 1 (2026-08-17): the client-side {@code compute.source.idle.
 * at.tail} DELTA mirror was removed with ComputeOtlpEmitter — the episode
 * latch + WARN/INFO logs remain the observable contract, and the native idle
 * signal moved to the source operator's {@code numRecordsOutPerSecond} meter
 * and {@code currentOutputWatermark} gauge. Tests assert the latch (one alert
 * per episode) instead of the drained delta.
 */
class SourceIdleWatchdogGeneratorTest {

    private final AtomicLong clock = new AtomicLong(1_000_000L);

    private WatermarkGenerator<RowData> generator;

    @BeforeEach
    void setUp() {
        SourceIdleWatchdogGenerator.resetEpisodeForTest();
        generator =
                new SourceIdleWatchdogGenerator(
                        CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L),
                        60_000L,
                        clock::get);
    }

    @AfterEach
    void tearDown() {
        SourceIdleWatchdogGenerator.resetEpisodeForTest();
    }

    @Test
    void noAlertWhileRecordsFlow() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 10_000L); // 10 s gap — well under 60 s
        generator.onEvent(null, 11_000L, output);
        generator.onPeriodicEmit(output);
        generator.onPeriodicEmit(output);

        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "no idle episode while records flow");
        // Watermark emission unchanged: bounded-out-of-orderness still emits
        // 4_999 then 5_999 (event-driven, tracker-14 design).
        assertEquals(java.util.List.of(4_999L, 5_999L), output.timestamps);
    }

    @Test
    void alertsOnceAfterThresholdWhenIdle() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L); // cross the 60 s threshold

        generator.onPeriodicEmit(new RecordingOutput());

        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "episode latch must be set after the first alert");
    }

    @Test
    void noRepeatWhileStillIdle() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest());

        // Keep idling — later periodic ticks must NOT re-alert (one per episode).
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "no repeat alerts inside one idle episode");
    }

    @Test
    void recordArrivalResumesAndRearmsNextEpisode() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest());

        // A record arrives: episode ends, latch re-arms.
        clock.set(clock.get() + 5_000L);
        generator.onEvent(null, 12_000L, new RecordingOutput());
        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "a record must clear the episode latch (resume)");

        // New idle episode after the resume must alert again.
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "a fresh idle episode after resume must alert again");
    }

    @Test
    void belowThresholdDoesNotAlert() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 59_999L); // 1 ms under the 60 s threshold
        generator.onPeriodicEmit(new RecordingOutput());
        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest());
    }

    @Test
    void startIdleFromSourceOpenCountsTowardThreshold() {
        // A restored source at a frozen tail has NO first record: idle time
        // starts at generator creation (source open), so a frozen-tail restore
        // alerts even though onEvent never ran.
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "idle measured from source-open (no records ever) must alert");
    }

    @Test
    void watermarksStillFlowWhileIdle() {
        // The watchdog must never block watermark emission even when idle.
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(output);
        // The periodic emit still forwards the delegate's watermark.
        assertFalse(output.timestamps.isEmpty(),
                "periodic emits must still reach the delegate while idle");
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final java.util.List<Long> timestamps = new java.util.ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            timestamps.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
