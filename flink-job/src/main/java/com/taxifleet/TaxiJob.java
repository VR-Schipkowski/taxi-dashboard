package com.taxifleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taxifleet.functions.SpeedCalculator;
import com.taxifleet.models.TaxiLocation;
import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
                .name("Parse JSON");

        DataStream<TaxiSpeed> speedStream = locationStream
                .keyBy(taxi -> taxi.taxiId)
                .map(new SpeedCalculator())
                .name("Calculate Speed");

        speedStream.print();

        env.execute("Taxi Fleet Monitoring");
    }
}