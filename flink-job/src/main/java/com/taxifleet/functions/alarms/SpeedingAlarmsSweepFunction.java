package com.taxifleet.functions.alarms;

import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.util.Collector;

public class SpeedingAlarmsSweepFunction extends BaseAlarmsSweepFunction {
    @Override
    public void processElement(TaxiSpeed event, Context ctx, Collector<String> out) throws Exception {
        addAlarm(event, ctx, out);
    }
}