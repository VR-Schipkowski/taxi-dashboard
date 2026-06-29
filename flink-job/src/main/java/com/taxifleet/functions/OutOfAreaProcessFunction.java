package com.taxifleet.functions;

import com.taxifleet.helper.Helper;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfAreaProcessFunction
        extends KeyedProcessFunction<Integer, TaxiSpeed, TaxiSpeed> {

    private static final Logger LOG = LoggerFactory.getLogger(OutOfAreaProcessFunction.class);

    private static final double FORBIDDEN_CITY_LATITUDE = 39.9163;
    private static final double FORBIDDEN_CITY_LONGITUDE = 116.3972;

    @Override
    public void processElement(
            TaxiSpeed current,
            Context ctx,
            Collector<TaxiSpeed> out) throws Exception {

        double MAXDISTANCE_FROM_CITY_KM = 25.0;

        double distance = Helper.calculateDistance(
                current.latitude,
                current.longitude,
                FORBIDDEN_CITY_LATITUDE,
                FORBIDDEN_CITY_LONGITUDE);

        if (distance > MAXDISTANCE_FROM_CITY_KM) {
            LOG.warn(
                    "Taxi {} is out of area! Distance: {} km",
                    current.taxiId,
                    distance);

            current.isOutOfArea = true;

            // Side output
            ctx.output(OutOfAreaProcess.OUT_OF_AREA_TAG, current);

        } else {
            current.isOutOfArea = false;
            out.collect(current);
        }
    }
}