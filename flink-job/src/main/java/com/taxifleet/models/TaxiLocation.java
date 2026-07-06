package com.taxifleet.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaxiLocation {

    @JsonProperty("taxiid")
    public int taxiId;

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

    public TaxiLocation(int taxiId, String timestamp, double longitude, double latitude) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}