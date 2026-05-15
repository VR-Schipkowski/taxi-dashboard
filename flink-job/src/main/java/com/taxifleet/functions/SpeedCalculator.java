package com.taxifleet.functions;

import com.taxifleet.helper.Helper;
import com.taxifleet.models.TaxiLocation;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

//old, bad approach, up for deletion later
public class SpeedCalculator extends RichMapFunction<TaxiLocation, TaxiSpeed> {

    private transient ValueState<TaxiLocation> previousLocation;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<TaxiLocation> descriptor =
                new ValueStateDescriptor<>("previous-location", TaxiLocation.class);
        previousLocation = getRuntimeContext().getState(descriptor);
    }

    @Override
    public TaxiSpeed map(TaxiLocation current) throws Exception {
        TaxiLocation previous = previousLocation.value();

        double speed = 0.0;

        if (previous != null) {
            double timeDiffSeconds = Helper.calculateTimeDifferenceSeconds(previous.timestamp, current.timestamp);
            double timeDiffHours = Helper.calculateTimeDifferenceHours(previous.timestamp, current.timestamp);

            if (timeDiffSeconds > 10.){
                double distanceKm = Helper.calculateDistance(
                previous.latitude, previous.longitude,
                current.latitude, current.longitude);
                speed = distanceKm / timeDiffHours;
                previousLocation.update(current);
            }
        }
        else {
            previousLocation.update(current);
        }

        return new TaxiSpeed(
                current.taxiId,
                current.timestamp,
                current.longitude,
                current.latitude,
                speed
        );
    }
}