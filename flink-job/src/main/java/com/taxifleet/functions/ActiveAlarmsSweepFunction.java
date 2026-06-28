package com.taxifleet.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ActiveAlarmsSweepFunction
        extends KeyedProcessFunction<Integer, TaxiSpeed, String> {

    private static final long TTL_MS = 5 * 60 * 1000L;
    private static final long SWEEP_INTERVAL_MS = 30_000L;

    private transient MapState<Integer, TaxiSpeed> activeAlarms; // taxiId -> latest event
    private transient MapState<Integer, Long> lastSeen;          // taxiId -> last update time
    private transient ValueState<Boolean> sweepScheduled;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        activeAlarms = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("active-alarms", Integer.class, TaxiSpeed.class));
        lastSeen = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("last-seen", Integer.class, Long.class));
        sweepScheduled = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sweep-scheduled", Boolean.class));
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(TaxiSpeed event, Context ctx, Collector<String> out) throws Exception {
        activeAlarms.put(event.taxiId, event);
        lastSeen.put(event.taxiId, ctx.timerService().currentProcessingTime());

        emitSnapshot(out);

        if (sweepScheduled.value() == null) {
            sweepScheduled.update(true);
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + SWEEP_INTERVAL_MS);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        List<Integer> stale = new ArrayList<>();

        for (Map.Entry<Integer, Long> e : lastSeen.entries()) {
            if (now - e.getValue() >= TTL_MS) stale.add(e.getKey());
        }
        for (Integer taxiId : stale) {
            activeAlarms.remove(taxiId);
            lastSeen.remove(taxiId);
        }
        if (!stale.isEmpty()) emitSnapshot(out);

        boolean stillActive = lastSeen.keys().iterator().hasNext();
        if (stillActive) {
            ctx.timerService().registerProcessingTimeTimer(now + SWEEP_INTERVAL_MS);
        } else {
            sweepScheduled.clear();
        }
    }

    private void emitSnapshot(Collector<String> out) throws Exception {
        List<TaxiSpeed> snapshot = new ArrayList<>();
        for (TaxiSpeed t : activeAlarms.values()) snapshot.add(t);
        out.collect(mapper.writeValueAsString(snapshot));
    }
}