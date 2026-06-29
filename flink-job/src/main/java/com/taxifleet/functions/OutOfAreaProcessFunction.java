package com.taxifleet.functions;

import com.taxifleet.helper.GeoFence;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfAreaProcessFunction
        extends KeyedProcessFunction<Integer, TaxiSpeed, TaxiSpeed> {

    private static final Logger LOG = LoggerFactory.getLogger(OutOfAreaProcessFunction.class);

    @Override
    public void processElement(
            TaxiSpeed current,
            Context ctx,
            Collector<TaxiSpeed> out) throws Exception {

        // Single source of truth: GeoFence defines the out-of-area rule
        // (distance from the city centre). Using it here keeps this check
        // identical to the flag set in the TaxiSpeed constructor, so the
        // side-output alerts and the taxi-processed payload never disagree.
        boolean outOfArea = GeoFence.isOutOfArea(current.latitude, current.longitude);
        current.isOutOfArea = outOfArea;

        if (outOfArea) {
            LOG.warn("Taxi {} is out of area at ({}, {})",
                    current.taxiId, current.latitude, current.longitude);

            // Side output → taxi-area-violations alert topic
            ctx.output(OutOfAreaProcess.OUT_OF_AREA_TAG, current);
        } else {
            out.collect(current);
        }
    }
}