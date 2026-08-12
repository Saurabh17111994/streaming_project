package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class CandleWatermarkStrategyTest {

    @Test
    void emitsBoundedWatermarksWhenNewEventTimeArrives() {
        WatermarkGenerator<RowData> generator = CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L);
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(null, 10_000L, output);
        generator.onEvent(null, 9_000L, output);
        generator.onEvent(null, 10_002L, output);
        generator.onPeriodicEmit(output);

        assertEquals(List.of(4_999L, 5_001L), output.timestamps);
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final List<Long> timestamps = new ArrayList<>();

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
