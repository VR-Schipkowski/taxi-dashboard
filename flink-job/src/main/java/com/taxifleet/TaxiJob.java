package com.taxifleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taxifleet.functions.*;
//TODO: not used
import com.taxifleet.helper.RedisSink;
import com.taxifleet.models.TaxiLocation;
import com.taxifleet.models.TaxiSpeed;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.*;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import com.taxifleet.models.HeatmapCell;

public class TaxiJob {
        private static final String BOOTSTRAP_SERVERS = "kafka:9092";
        private static final Duration WINDOW_DURATION = Duration.ofSeconds(5);
        private static final Duration WATERMARK_BOUND = Duration.ofSeconds(2);

        private static KafkaSink<String> createKafkaSink(String topic) {
                return KafkaSink.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .build();
        }

        private static DataStream<String> createKafkaSource(StreamExecutionEnvironment env) {
                KafkaSource<String> source = KafkaSource.<String>builder()
                                .setBootstrapServers(BOOTSTRAP_SERVERS)
                                .setTopics("taxi-locations")
                                .setGroupId("flink-taxi-consumer")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();
                return env.fromSource(
                        source,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Taxi Source"
                );
        }

        private static DataStream<TaxiLocation> parseLocations(DataStream<String> rawStream) {
                return rawStream
                        .flatMap(new LocationParser())
                        .name("Parse JSON and Filter Invalid Locations")
                        .assignTimestampsAndWatermarks(
                                        WatermarkStrategy.<TaxiLocation>forBoundedOutOfOrderness(WATERMARK_BOUND)
                                                .withTimestampAssigner((loc,recordTimestamp) -> loc.eventTimeMillis));                            
        }

        private static DataStream<TaxiLocation> throttleLocations(DataStream<TaxiLocation> locations) {
                return locations
                        .keyBy(location -> location.taxi_id)
                        .window(TumblingProcessingTimeWindows.of(WINDOW_DURATION))
                        .maxBy("timestamp");
        }

        private static SingleOutputStreamOperator<TaxiSpeed> calculateSpeed(DataStream<TaxiLocation> filteredLocationStream) {
                return filteredLocationStream
                        .keyBy(location -> location.taxi_id)
                        .process(new SpeedCalculatorProcessFunction());  
        }

        private static void storeToRedis(SingleOutputStreamOperator<TaxiSpeed> speedStream) {
               speedStream
                        .process(new RedisSinkFunction())
                        .name("Store Information to Redis");
        }
        // TODO: currently we do not have parallelism since active alarmssweepfunction cannot handle it
        private static void processOOAViolations(SingleOutputStreamOperator<TaxiSpeed> speedStream,
                KafkaSink<String> violationsSink) {
                SingleOutputStreamOperator<TaxiSpeed> outOfAreaCheckedStream = speedStream
                        .keyBy(speed -> speed.taxi_id)
                        .process(new OutOfAreaProcessFunction());
                DataStream<TaxiSpeed> outOfAreaStream = outOfAreaCheckedStream
                        .getSideOutput(OutOfAreaProcess.OUT_OF_AREA_TAG);
                DataStream<String> areaSnapshot = outOfAreaStream
                        .keyBy(speed -> 0)
                        .process(new ActiveAlarmsSweepFunction())
                        .setParallelism(1)
                        .name("Area Active Alarms Snapshot");
                areaSnapshot.sinkTo(violationsSink).name("Notify Area Violation");
        }
        // TODO: currently we do not have parallelism since active alarmssweepfunction cannot handle it
        private static void processSpeedingViolations(SingleOutputStreamOperator<TaxiSpeed> speedStream,
                KafkaSink<String> speedingSink) {
                        DataStream<TaxiSpeed> speedingStream = speedStream
                                .getSideOutput(SpeedCalculatorProcess.SPEEDING_TAG);
                        DataStream<String> speedingSnapshot = speedingStream
                                .keyBy(speed -> 0)
                                .process(new ActiveAlarmsSweepFunction())
                                .setParallelism(1)
                                .name("Speeding Active Alarms Snapshot");
                        speedingSnapshot.sinkTo(speedingSink).name("Notify Speeding");

        }
        private static void processHeatmap(DataStream<TaxiSpeed> locationStream, KafkaSink<String> heatmapSink, ObjectMapper mapper) {
                // Heatmap — distinct taxi count per cell, sliding window
                DataStream<HeatmapCell> heatmapStream = HeatmapPipeline.build(locationStream);
                
                heatmapStream.map(mapper::writeValueAsString).sinkTo(heatmapSink)
                                .name("Heatmap Distinct Taxi Count per Cell");
}

        private static void propagateToDashboard(SingleOutputStreamOperator<TaxiSpeed> outOfAreaCheckedStream,
                KafkaSink<String> processedSink, ObjectMapper mapper) {
                        outOfAreaCheckedStream
                                .map(mapper::writeValueAsString)
                                .sinkTo(processedSink)
                                .name("Propagate Location to Dashboard");
        }

        // ----------------- main function
        public static void main(String[] args) throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                System.out.println("Flink Taxi Job starting - connecting to Kafka...");

                ObjectMapper mapper = new ObjectMapper();
                KafkaSink<String> violationsSink = createKafkaSink("taxi-area-violations");
                KafkaSink<String> processedSink = createKafkaSink("taxi-processed");
                KafkaSink<String> speedingSink = createKafkaSink("taxi-speeding");
                KafkaSink<String> heatmapSink = createKafkaSink("taxi-heatmap");

                // data source
                DataStream<String> kafkaStream = createKafkaSource(env);

                // reordering incomming data, filtering out invalid data, parsing the json into TaxiLocation objects
                DataStream<TaxiLocation> locationStream = parseLocations(kafkaStream);
                // windowing, to only get one update per taxi per 5 seconds using the latest location
                DataStream<TaxiLocation> filteredLocationStream = throttleLocations(locationStream);
                SingleOutputStreamOperator<TaxiSpeed> speedStream = calculateSpeed(filteredLocationStream);

                storeToRedis(speedStream);
                processOOAViolations(speedStream, violationsSink);
                processSpeedingViolations(speedStream, speedingSink);

                //TODO: I think we create the stream twice once here and once in the processOOViolations, maybe better either to use the side stream as a parameter or return the main stream
                SingleOutputStreamOperator<TaxiSpeed> outOfAreaCheckedStream = speedStream
                        .keyBy(speed -> speed.taxi_id)
                        .process(new OutOfAreaProcessFunction());

                propagateToDashboard(outOfAreaCheckedStream, processedSink, mapper);

                processHeatmap(outOfAreaCheckedStream, heatmapSink, mapper);
                

                

                env.execute("Taxi Fleet Monitoring");
        }


}
