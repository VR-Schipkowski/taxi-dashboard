package com.taxifleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taxifleet.functions.*;
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

public class TaxiJob {
    private static String bootstrapServers = "kafka:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Flink Taxi Job starting - connecting to Kafka...");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("taxi-locations")
                .setGroupId("flink-taxi-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Taxi Source");

        DataStream<TaxiLocation> locationStream = kafkaStream
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(json, TaxiLocation.class);
                })
                .filter(location -> location.latitude >= -90 &&
                        location.latitude <= 90 &&
                        location.longitude >= -180 &&
                        location.longitude <= 180 &&
                        location.latitude != 0.0 &&
                        location.longitude != 0.0)
                .name("Parse JSON + Watermarks");

        DataStream<TaxiLocation> filteredLocationStream = locationStream
                .keyBy(location -> location.taxiId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .maxBy("timestamp");

        SingleOutputStreamOperator<TaxiSpeed> speedStream = filteredLocationStream
                .keyBy(location -> location.taxiId)
                .process(new SpeedCalculatorProcessFunction());

        ObjectMapper mapper = new ObjectMapper();

        // Store Information — Flink writes every event directly to Redis (professor's topology)
        RedisSink redisSink = new RedisSink("redis", 6379);
        speedStream.process(new org.apache.flink.streaming.api.functions.ProcessFunction<TaxiSpeed, Void>() {
            @Override
            public void processElement(TaxiSpeed speed,
                    org.apache.flink.streaming.api.functions.ProcessFunction<TaxiSpeed, Void>.Context ctx,
                    org.apache.flink.util.Collector<Void> out) {
                redisSink.store(speed);
            }
        }).name("Store Information to Redis");

        // Propagate location to dashboard — throttled to one update per taxi per 5 seconds
        DataStream<TaxiSpeed> throttledStream = speedStream
                .keyBy(speed -> speed.taxiId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .maxBy("timestamp");

        KafkaSink<String> processedSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("taxi-processed")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        throttledStream.map(mapper::writeValueAsString).sinkTo(processedSink).name("Propagate Location to Dashboard");

        // Notify speeding — immediate, side output from SpeedCalculatorProcessFunction
        DataStream<TaxiSpeed> speedingStream = speedStream.getSideOutput(SpeedCalculatorProcess.SPEEDING_TAG);
        KafkaSink<String> speedingSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("taxi-speeding")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        speedingStream.map(mapper::writeValueAsString).sinkTo(speedingSink).name("Notify Speeding");

        // Notify area violation — immediate, side output from OutOfAreaProcessFunction
        SingleOutputStreamOperator<TaxiSpeed> outOfAreaCheckedStream = speedStream
                .keyBy(speed -> speed.taxiId)
                .process(new OutOfAreaProcessFunction());
        DataStream<TaxiSpeed> outOfAreaStream = outOfAreaCheckedStream.getSideOutput(OutOfAreaProcess.OUT_OF_AREA_TAG);
        KafkaSink<String> violationsSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("taxi-area-violations")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        outOfAreaStream.map(mapper::writeValueAsString).sinkTo(violationsSink).name("Notify Area Violation");

        env.execute("Taxi Fleet Monitoring");
    }
}
