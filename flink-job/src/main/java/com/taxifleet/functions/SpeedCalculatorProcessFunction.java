package com.taxifleet.functions;

import com.taxifleet.helper.Helper;
import com.taxifleet.models.TaxiLocation;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpeedCalculatorProcessFunction
                extends KeyedProcessFunction<Integer, TaxiLocation, TaxiSpeed> {

        private static final Logger LOG = LoggerFactory.getLogger(SpeedCalculatorProcessFunction.class);

        private static final double SPEEDLIMIT = 60.0;
        private static final int WARMUP = 3;
        private static final double MAXSPEED = 150.0; // realistic taxi limit buffer
        private static final double PARKING = 180; // 3 minutes

        private transient ValueState<Double> avarageTaxiSpeedKmh;
        private transient ValueState<Integer> count;
        private transient ValueState<Double> totalDistanceKm;
        private transient ValueState<Integer> speedSampleCount;
        private transient ValueState<TaxiLocation> previousLocation;
        private transient ValueState<String> lastMoved;

        @Override
        public void open(Configuration parameters) {
                ValueStateDescriptor<TaxiLocation> descriptor = new ValueStateDescriptor<>("previous-location",
                                TaxiLocation.class);

                previousLocation = getRuntimeContext().getState(descriptor);
                count = getRuntimeContext().getState(new ValueStateDescriptor<>("init-count", Integer.class));
                avarageTaxiSpeedKmh = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("average-speed-kmh", Double.class));
                totalDistanceKm = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("total-distance-km", Double.class));
                speedSampleCount = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("speed-sample-count", Integer.class));
                lastMoved = getRuntimeContext().getState(new ValueStateDescriptor<>("last-moved", String.class));
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
                if (avarageTaxiSpeedKmh.value() == null)
                        avarageTaxiSpeedKmh.update(0.0);
                if (c == null)
                        c = 0;
                if (totalDistanceKm.value() == null)
                        totalDistanceKm.update(0.0);
                if (lastMoved.value() == null)
                        lastMoved.update("1897-02-02 13:33:08");
                if (previous == null) {
                        count.update(0);
                        previousLocation.update(current);

                        out.collect(new TaxiSpeed(
                                        current.taxiId,
                                        current.timestamp,
                                        current.longitude,
                                        current.latitude,
                                        0.0,
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
                                        0.0, 0.0));
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
                                totalDistanceKm.value() == null ? 0.0 : totalDistanceKm.value(),
                                avarageTaxiSpeedKmh.value() == null ? 0.0 : avarageTaxiSpeedKmh.value());

                if (speed > SPEEDLIMIT) {
                        ctx.output(SpeedCalculatorProcess.SPEEDING_TAG, result);
                }
                if (speed > 0.5) {
                        lastMoved.update(current.timestamp);
                        result.lastMoved = current.timestamp;
                        result.isParking = false;
                } else {
                        String lastMovedTs = lastMoved.value();

                        double parkedSeconds = Helper.calculateTimeDifferenceSeconds(lastMovedTs,
                                        current.timestamp);
                        if (parkedSeconds > PARKING) {
                                result.isParking = true;
                        }
                        result.lastMoved = lastMovedTs;
                }

                out.collect(result);

                // Update distance
                double prevDist = totalDistanceKm.value() == null ? 0.0 : totalDistanceKm.value();
                double legDist = Helper.calculateDistance(
                                previous.latitude, previous.longitude,
                                current.latitude, current.longitude);
                totalDistanceKm.update(prevDist + legDist);

                // Update rolling average speed
                Integer samples = speedSampleCount.value();
                if (samples == null)
                        samples = 0;
                double prevAvg = avarageTaxiSpeedKmh.value() == null ? 0.0 : avarageTaxiSpeedKmh.value();
                double newAvg = (prevAvg * samples + speed)
                                / (samples + 1);
                avarageTaxiSpeedKmh.update(newAvg);
                speedSampleCount.update(samples + 1);

                // IMPORTANT: only update state after validation
                previousLocation.update(current);
        }
}