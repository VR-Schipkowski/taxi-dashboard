package com.taxifleet.functions;

import com.taxifleet.helper.GeoFence;
import com.taxifleet.models.TaxiLocation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfAreaProcessFunction
        extends KeyedProcessFunction<Integer, TaxiLocation, TaxiLocation> {

    private static final Logger LOG = LoggerFactory.getLogger(OutOfAreaProcessFunction.class);

    private transient ValueState<Boolean> previousOOAState;

    @Override
    public void open(Configuration parameters) {
        previousOOAState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previous-ooa-state", Boolean.class));
    }

    @Override
    public void processElement(
            TaxiLocation current,
            Context ctx,
            Collector<TaxiLocation> out) throws Exception {

        // Single source of truth: GeoFence defines the out-of-area rule
        // (distance from the city centre). Using it here keeps this check
        // identical to the flag set in the TaxiSpeed constructor, so the
        // side-output alerts and the taxi-processed payload never disagree.
        boolean outOfArea = GeoFence.isOutOfArea(current.latitude, current.longitude);

        Boolean prev = previousOOAState.value();
        boolean wasOutOfArea = prev != null && prev;
        boolean stateChanged = (outOfArea != wasOutOfArea);
        previousOOAState.update(outOfArea);


       if (outOfArea) {
            LOG.warn("Taxi {} is out of area at ({}, {})", current.taxi_id, current.latitude, current.longitude);
            ctx.output(OutOfAreaProcess.OUT_OF_AREA_TAG, current);
        }
       else{
           if (stateChanged) {
               ctx.output(OutOfAreaProcess.OOA_RETURNED_TAG, current);
           }
           out.collect(current);
       }
    }
}