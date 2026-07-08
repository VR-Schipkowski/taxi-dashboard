package com.taxifleet.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taxifleet.models.TaxiLocation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class LocationParser extends RichFlatMapFunction<String, TaxiLocation> {

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String json, Collector<TaxiLocation> out) {
        TaxiLocation loc;
        try {
            loc = mapper.readValue(json, TaxiLocation.class);
        } catch (Exception e) {
            // Covers the __END__ token payload too (no timestamp/lon/lat),
            // as well as any genuinely malformed JSON.
            // TODO: side-output for observability instead of silent drop.
            return;
        }

        if (loc.timestamp == null) {
            // e.g. __END__ token records - no timestamp field present.
            return;
        }

        try {
            loc.eventTimeMillis = LocalDateTime
                    .parse(loc.timestamp, TS_FORMAT)
                    .atZone(ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception e) {
            // Malformed timestamp string - drop, don't crash the job.
            return;
        }

        if (isValid(loc)) {
            out.collect(loc);
        }
    }

    private boolean isValid(TaxiLocation l) {
        return l.latitude >= -90 && l.latitude <= 90
                && l.longitude >= -180 && l.longitude <= 180
                && l.latitude != 0.0 && l.longitude != 0.0;
    }
}