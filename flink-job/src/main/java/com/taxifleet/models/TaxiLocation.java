package com.taxifleet.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaxiLocation {

    @JsonProperty("taxi_id")
    public int taxiId;

    public String timestamp;
    public double longitude;
    public double latitude;

    public TaxiLocation() {}

    public TaxiLocation(int taxiId, String timestamp, double longitude, double latitude) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}