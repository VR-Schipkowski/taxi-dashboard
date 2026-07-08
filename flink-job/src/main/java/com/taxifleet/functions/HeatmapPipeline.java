package com.taxifleet.functions;

import com.taxifleet.models.HeatmapCell;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class HeatmapPipeline {
    public static DataStream<HeatmapCell> build(DataStream<TaxiSpeed> locationStream) {
        return locationStream
                .keyBy(location -> GridUtil.cellFor(location.latitude, location.longitude))
                .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new DistinctTaxiCountAggregator(),
                        new ProcessWindowFunction<Integer, HeatmapCell, String, TimeWindow>() {
                            @Override
                            public void process(String cellId, Context ctx, Iterable<Integer> counts,
                                    Collector<HeatmapCell> out) {
                                out.collect(new HeatmapCell(cellId, counts.iterator().next(),
                                        ctx.window().getStart(), ctx.window().getEnd()));
                            }
                        })
                .name("Heatmap Distinct Taxi Count per Cell");
    }
}