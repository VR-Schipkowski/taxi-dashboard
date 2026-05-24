package com.taxifleet.models;

public class TaxiSpeed {

    public int taxiId;
    public String timestamp;
    public double longitude;
    public double latitude;
    public double speed;
    public double totalDistance;
    public boolean isSpeeding;

    public TaxiSpeed() {}

    public TaxiSpeed(int taxiId, String timestamp, double longitude, double latitude) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public TaxiSpeed(int taxiId, String timestamp, double longitude, double latitude, double speed, double totalDistance) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        this.speed = speed;
        this.totalDistance = totalDistance;
        this.isSpeeding = speed>60;
    }

    @Override
    public String toString() {
        String r = String.format("Taxi %d: %.2f km/h at (%.5f, %.5f)",
                taxiId, speed, latitude, longitude);
        if(isSpeeding) r += " !!!Speeding!!!";
        return r;
    }
}
