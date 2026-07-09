package com.taxifleet.functions.heatmap;

import com.taxifleet.models.HeatmapCell;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HeatmapWindowProcessor extends ProcessWindowFunction<Integer, HeatmapCell, String, TimeWindow> {
    @Override
    public void process(String cellId, Context ctx, Iterable<Integer> counts, Collector<HeatmapCell> out) {
        out.collect(new HeatmapCell(cellId, counts.iterator().next(), ctx.window().getStart(), ctx.window().getEnd()));
    }
}