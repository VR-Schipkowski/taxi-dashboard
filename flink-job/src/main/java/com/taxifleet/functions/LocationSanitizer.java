package com.taxifleet.functions;

import com.taxifleet.helper.Helper;
import com.taxifleet.models.TaxiLocation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LocationSanitizer
        extends KeyedProcessFunction<Integer, TaxiLocation, TaxiLocation> {

    private transient ValueState<TaxiLocation> previous;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<TaxiLocation> descriptor =
                new ValueStateDescriptor<>(
                        "previous-location",
                        TaxiLocation.class
                );

        previous = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            TaxiLocation current,
            Context ctx,
            Collector<TaxiLocation> out
    ) throws Exception {

        TaxiLocation prev = previous.value();

        if (prev == null) {
            previous.update(current);
            out.collect(current);
            return;
        }

        double distance = Helper.calculateDistance(
                prev.latitude,
                prev.longitude,
                current.latitude,
                current.longitude
        );

        double hours = Helper.calculateTimeDifferenceHours(
                prev.timestamp,
                current.timestamp
        );

        if (hours <= 0) {
            return; //reject duplicate and out of order events
        }

        double speed = distance / hours;

        if (speed < 200) {
            out.collect(current); //reject impossible speeds, usually if someone moves at speed more than 200 km/h it's a mistake
            previous.update(current);
        }
    }
}