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
                        Duration.ofMinutes(1),
                        Duration.ofSeconds(20)
                ))
                .process(new TotalDistanceSpeedCalculator())
                .name("Total Distance Speed Calculator");
                // Store processed results in Redis
                speedStream.process(new SpeedRedisProcessor("redis", 6379))
                .name("Store Speed in Redis");
                
                speedStream.print();

        env.execute("Taxi Fleet Monitoring");
    }
}