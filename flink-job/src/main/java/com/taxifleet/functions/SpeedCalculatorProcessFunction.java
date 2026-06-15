package com.taxifleet.functions;

import com.taxifleet.helper.Helper;
import com.taxifleet.models.TaxiLocation;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpeedCalculatorProcessFunction
                extends KeyedProcessFunction<Integer, TaxiLocation, TaxiSpeed> {

        private static final Logger LOG = LoggerFactory.getLogger(SpeedCalculatorProcessFunction.class);

        private static final double SPEEDLIMIT = 50.0;
        private static final int WARMUP = 3;
        private static final double MAXSPEED = 180.0; // realistic taxi limit buffer

        private transient ValueState<Integer> count;

        private transient ValueState<TaxiLocation> previousLocation;

        @Override
        public void open(Configuration parameters) {
                ValueStateDescriptor<TaxiLocation> descriptor = new ValueStateDescriptor<>("previous-location",
                                TaxiLocation.class);

                previousLocation = getRuntimeContext().getState(descriptor);
                count = getRuntimeContext().getState(new ValueStateDescriptor<>("init-count", Integer.class));
        }

        public static double speedCalc(
                        TaxiLocation prev,
                        TaxiLocation curr,
                        double timeDiffSeconds) {

                if (prev == null)
                        return 0.0;

                double distance = Helper.calculateDistance(
                                prev.latitude, prev.longitude,
                                curr.latitude, curr.longitude);

                double speed = distance * 3600.0 / timeDiffSeconds;

                return speed;
        }

        @Override
        public void processElement(
                        TaxiLocation current,
                        Context ctx,
                        Collector<TaxiSpeed> out) throws Exception {

                TaxiLocation previous = previousLocation.value();

                Integer c = count.value();
                if (c == null)
                        c = 0;

                if (previous == null) {
                        count.update(0);
                        previousLocation.update(current);

                        out.collect(new TaxiSpeed(
                                        current.taxiId,
                                        current.timestamp,
                                        current.longitude,
                                        current.latitude,
                                        0.0,
                                        0.0));

                        return;
                }

                double timeDiffSeconds = Helper.calculateTimeDifferenceSeconds(previous.timestamp, current.timestamp);
                if (timeDiffSeconds <= 0) {
                        LOG.warn("DROP_INVALID_TIME taxiId={} prevTs={} currTs={}",
                                        current.taxiId, previous.timestamp, current.timestamp);
                        return;
                }
                if (timeDiffSeconds > 300) {
                        LOG.warn("DROP_OLD_DATA taxiId={} timeDiffSec={}", current.taxiId, timeDiffSeconds);
                        previousLocation.update(current);
                        count.update(1); // reset warmup after long gap

                        out.collect(new TaxiSpeed(
                                        current.taxiId,
                                        current.timestamp,
                                        current.longitude,
                                        current.latitude,
                                        0.0,
                                        0.0));
                        return;
                }
                double speed = speedCalc(previous, current, timeDiffSeconds);

                if (c < WARMUP) {
                        if (speed <= MAXSPEED) {
                                previousLocation.update(current);
                                count.update(c + 1);

                        }
                        out.collect(new TaxiSpeed(
                                        current.taxiId,
                                        current.timestamp,
                                        current.longitude,
                                        current.latitude,
                                        0.0,
                                        0.0));
                        return;
                }

                if (speed > MAXSPEED) {

                        LOG.warn("SPIKE_REJECTED taxiId={} speed={} km/h distKm={} timeSec={}",
                                        current.taxiId, speed, timeDiffSeconds);

                        // IMPORTANT:
                        // do NOT update state → prevents poisoning future calculations
                        return;
                }

                TaxiSpeed result = new TaxiSpeed(
                                current.taxiId,
                                current.timestamp,
                                current.longitude,
                                current.latitude,
                                speed,
                                0.0);

                if (speed > SPEEDLIMIT) {
                        ctx.output(SpeedCalculatorProcess.SPEEDING_TAG, result);
                }

                out.collect(result);

                // IMPORTANT: only update state after validation
                previousLocation.update(current);
        }
}