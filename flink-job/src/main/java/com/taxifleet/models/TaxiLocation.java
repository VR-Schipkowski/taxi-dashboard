package com.taxifleet.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaxiLocation {

    @JsonProperty("taxi_id")
    public int taxi_id;

    public String timestamp;
    public double longitude;
    public double latitude;

    // Wall-clock epoch-millis stamped by the provider when this event was
    // published to Kafka. Carried through the pipeline to measure end-to-end
    // latency.
    @JsonProperty("ingested_at")
    public long ingestedAt;
    public long eventTimeMillis;

    public TaxiLocation() {
    }

    public TaxiLocation(int taxi_id, String timestamp, double longitude, double latitude) {
        this.taxi_id = taxi_id;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}