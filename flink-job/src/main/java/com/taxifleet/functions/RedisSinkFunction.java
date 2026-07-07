package com.taxifleet.functions;

import com.taxifleet.helper.RedisSink;
import com.taxifleet.models.TaxiSpeed;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class RedisSinkFunction extends ProcessFunction<TaxiSpeed, Void> {
    private transient RedisSink redisSink;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisSink = new RedisSink("redis", 6379);
    }

    @Override
    public void processElement( TaxiSpeed speed, Context ctx, Collector<Void> out) throws Exception {
        redisSink.store(speed);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}