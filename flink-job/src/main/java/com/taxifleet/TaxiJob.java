package com.taxifleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taxifleet.functions.*;
import com.taxifleet.models.TaxiLocation;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;


import java.time.Duration;

public class TaxiJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Flink Taxi Job starting - connecting to Kafka...");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
//                .setBootstrapServers("localhost:9092")
                .setTopics("taxi-locations")
                .setGroupId("flink-taxi-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Taxi Source"
        );

        DataStream<TaxiLocation> locationStream = kafkaStream
        .map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, TaxiLocation.class);
        })
        .filter(location ->
                location.latitude >= -90 &&
                location.latitude <= 90 &&
                location.longitude >= -180 &&
                location.longitude <= 180 &&
                location.latitude != 0.0 &&
                location.longitude != 0.0
        )
        .name("Parse JSON");

        DataStream<TaxiSpeed> speedStream = locationStream
                .keyBy(location -> location.taxiId)
                .process(new LocationSanitizer())
                .keyBy(location -> location.taxiId)
                .window(SlidingProcessingTimeWindows.of(
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(5)
                ))
                .process(new TotalDistanceSpeedCalculator())
                .name("Total Distance Speed Calculator");
        
        // Convert TaxiSpeed to JSON string for all processed events
        DataStream<String> processedJson = speedStream
        .map(speed -> new ObjectMapper().writeValueAsString(speed))
        .name("Serialize to JSON");

        // Sink 1: all processed events
        KafkaSink<String> processedSink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("taxi-processed")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .build();
        processedJson.sinkTo(processedSink);

        // Sink 2: speeding events only
        KafkaSink<String> speedingSink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("taxi-speeding")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .build();
        speedStream.filter(s -> s.isSpeeding)
        .map(speed -> new ObjectMapper().writeValueAsString(speed))
        .sinkTo(speedingSink);

        // Sink 3: area violation events only
        KafkaSink<String> violationsSink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("taxi-area-violations")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .build();
        speedStream.filter(s -> s.isOutOfArea)
        .map(speed -> new ObjectMapper().writeValueAsString(speed))
        .sinkTo(violationsSink);

                
        speedStream.print();

        env.execute("Taxi Fleet Monitoring");
    }
}