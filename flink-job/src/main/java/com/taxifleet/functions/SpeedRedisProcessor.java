package com.taxifleet.functions;

import com.taxifleet.helper.RedisSink;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SpeedRedisProcessor extends ProcessFunction<TaxiSpeed, Void> {
    private final RedisSink redisSink;

    public SpeedRedisProcessor(String redisHost, int redisPort) {
        this.redisSink = new RedisSink(redisHost, redisPort);
    }

    @Override
    public void processElement(
        TaxiSpeed speed,
        Context ctx,
        Collector<Void> out
    ) {
        redisSink.storeTaxiSpeed(speed);
    }
}