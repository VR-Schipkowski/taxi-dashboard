package com.taxifleet.functions;

import com.taxifleet.models.HeatmapCell;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ZeroFillProcessFunction extends KeyedProcessFunction<String, HeatmapCell, HeatmapCell> {

    private final long checkIntervalMs;
    private final long windowSizeMs;
    private transient ValueState<Long> lastActivityTimestamp;

    /**
     * @param checkInterval The interval at which we check whether data is missing
     *                      (e.g. every 1 or 2 minutes).
     * @param windowSize    The window size (e.g. 10 minutes) used to generate
     *                      correct timestamps for the zero cell.
     */
    public ZeroFillProcessFunction(Duration checkInterval, Duration windowSize) {
        this.checkIntervalMs = checkInterval.toMillis();
        this.windowSizeMs = windowSize.toMillis();
    }

    @Override
    public void open(Configuration parameters) {
        lastActivityTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastActivity", Long.class));
    }

    @Override
    public void processElement(HeatmapCell value, Context ctx, Collector<HeatmapCell> out) throws Exception {
        long currentTime = ctx.timerService().currentProcessingTime();
        lastActivityTimestamp.update(currentTime);

        // Register the timer dynamically based on the interval
        ctx.timerService().registerProcessingTimeTimer(currentTime + checkIntervalMs);

        // Forward the real value as usual
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<HeatmapCell> out) throws Exception {
        Long lastActive = lastActivityTimestamp.value();
        long currentTime = ctx.timerService().currentProcessingTime();

        // Check whether the last real event is older than the configured interval
        if (lastActive == null || (currentTime - lastActive) >= checkIntervalMs) {
            // Generate the zero cell with the correct dynamic window timestamps
            out.collect(new HeatmapCell(
                    ctx.getCurrentKey(),
                    0,
                    currentTime - windowSizeMs,
                    currentTime));

            // Set the next timer for the follow-up check
            ctx.timerService().registerProcessingTimeTimer(currentTime + checkIntervalMs);
        }
    }
}
