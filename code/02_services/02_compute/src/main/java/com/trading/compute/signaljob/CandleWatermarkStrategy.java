package com.trading.compute.signaljob;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;

/**
 * Event-time strategy for the raw-tick stream (Signal dossier: bounded
 * out-of-orderness watermark on {@code event_time}, with source idleness).
 *
 * <p>{@code event_time} is the verified UTC broker timestamp in epoch
 * milliseconds (raw_table_1 DDL). {@code WATERMARK_OUT_OF_ORDER_MS} (default
 * 5000) bounds how far behind the watermark may lag the max seen event time;
 * {@code SOURCE_IDLE_MS} (default 15000) advances watermarks past Fluss
 * bucket splits that go quiet, so windows on idle instruments still close.
 */
public final class CandleWatermarkStrategy {

    private CandleWatermarkStrategy() {}

    public static WatermarkStrategy<RowData> of(SignalJobConfig config) {
        return WatermarkStrategy.<RowData>forGenerator(
                        context -> boundedOutOfOrderGenerator(config.outOfOrderMs()))
                .withIdleness(Duration.ofMillis(config.sourceIdleMs()))
                .withTimestampAssigner(
                        (row, timestamp) -> row.getLong(RawTableColumns.EVENT_TIME));
    }

    /**
     * Emits the normal bounded-out-of-orderness watermark as each new maximum event time
     * arrives. Flink's built-in generator emits only on its periodic timer; a replay can drain a
     * whole raw-log burst before that timer fires, incorrectly admitting an event that arrived
     * after a pusher in log order. Event-driven emission makes late-event handling independent of
     * replay speed. The surrounding {@link WatermarkStrategy#withIdleness(Duration)} still marks
     * inactive source splits idle.
     */
    static WatermarkGenerator<RowData> boundedOutOfOrderGenerator(long outOfOrderMs) {
        return new BoundedOutOfOrdernessOnEventGenerator(outOfOrderMs);
    }

    private static final class BoundedOutOfOrdernessOnEventGenerator
            implements WatermarkGenerator<RowData> {
        private final long outOfOrderMs;
        private long maxTimestamp;
        private long lastEmittedWatermark = Long.MIN_VALUE;

        private BoundedOutOfOrdernessOnEventGenerator(long outOfOrderMs) {
            this.outOfOrderMs = outOfOrderMs;
            this.maxTimestamp = Long.MIN_VALUE + outOfOrderMs + 1;
        }

        @Override
        public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            long nextWatermark = maxTimestamp - outOfOrderMs - 1;
            if (nextWatermark > lastEmittedWatermark) {
                output.emitWatermark(new Watermark(nextWatermark));
                lastEmittedWatermark = nextWatermark;
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Watermarks are emitted synchronously in onEvent.
        }
    }
}
