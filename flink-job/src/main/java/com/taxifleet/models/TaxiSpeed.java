package com.taxifleet.models;

public class TaxiSpeed {
    public int taxiId;
    public String timestamp;
    public double longitude;
    public double latitude;
    public double speed;
    public double averageSpeed;
    public double totalDistance;
    public boolean isSpeeding;
    public boolean isOutOfArea;
    public String lastMoved;
    public boolean isParking;

    // Wall-clock epoch-millis from the provider, carried through so consumers can
    // compute end-to-end pipeline latency (now - ingestedAt). Auto-serialized into
    // taxi-processed because Jackson includes all public fields.
    public long ingestedAt;

    public TaxiSpeed() {
    }

    public TaxiSpeed(int taxiId, String timestamp, double longitude, double latitude) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        // Removed, since this brings in doubled logic: this.isOutOfArea = com.taxifleet.helper.GeoFence.isOutOfArea(latitude, longitude);
    }

    public TaxiSpeed(int taxiId, String timestamp, double longitude, double latitude, double speed) {
        this(taxiId, timestamp, longitude, latitude, speed, 0.0, 0.0);
    }

    // TODO: remove logic and fuction calles from the models and move it to the
    // process function, models should be pure data objects
    public TaxiSpeed(int taxiId, String timestamp, double longitude, double latitude, double speed,
            double totalDistance, double averageSpeed) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        this.speed = speed;
        this.totalDistance = totalDistance;
        this.averageSpeed = averageSpeed;
        this.isSpeeding = speed > 60;
        // Removed, since this bring in doubled logic: this.isOutOfArea = com.taxifleet.helper.GeoFence.isOutOfArea(latitude, longitude);
    }

    @Override
    public String toString() {
        String r = String.format("Taxi %d: %.2f km/h at (%.5f, %.5f)",
                taxiId, speed, latitude, longitude);
        if (isSpeeding)
            r += " !!!Speeding!!!";
        if (isOutOfArea)
            r += " !!!OutOfArea!!!";
        return r;
    }
}