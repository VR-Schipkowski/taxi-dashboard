package com.taxifleet.functions;

import com.taxifleet.helper.RedisSink;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TotalDistanceFunction
        extends KeyedProcessFunction<Integer, TaxiSpeed, Double> {

    protected transient MapState<Integer, Double> DistanceDict;
    protected transient ValueState<Double> distanceTotal;
    private transient RedisSink redisSink;

    @Override
    public void open(Configuration parameters) {
        DistanceDict = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("distance-dict", Integer.class, Double.class));
        distanceTotal = getRuntimeContext().getState(new ValueStateDescriptor<>("distance-total", Double.class));
        redisSink = new RedisSink("redis", 6379);
    }

    @Override
    public void processElement(TaxiSpeed taxiSpeed, KeyedProcessFunction<Integer, TaxiSpeed, Double>.Context context, Collector<Double> out) throws Exception {
        Double totalDistance = distanceTotal.value();
        if (totalDistance == null) {
            totalDistance = 0.0;
        }
        double taxiDistance;
        if(!DistanceDict.contains(taxiSpeed.taxi_id)){
            taxiDistance = taxiSpeed.totalDistance;
        } else{
            taxiDistance = taxiSpeed.totalDistance - DistanceDict.get(taxiSpeed.taxi_id);
        }
        DistanceDict.put(taxiSpeed.taxi_id, taxiSpeed.totalDistance);
        totalDistance += taxiDistance;
        distanceTotal.update(totalDistance);
        out.collect(totalDistance);
        redisSink.storeTotal(totalDistance);
    }
}