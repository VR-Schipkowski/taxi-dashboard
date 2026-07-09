package com.taxifleet.functions.alarms;

import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.util.Collector;

public class OOAAlarmsSweepFunction extends BaseAlarmsSweepFunction {
    @Override
    public void processElement(TaxiSpeed event, Context ctx, Collector<String> out) throws Exception {
        if (event.isOutOfArea) {
            addAlarm(event, ctx, out);
        } else {
            removeAlarm(event.taxi_id, out);
        }
    }
}