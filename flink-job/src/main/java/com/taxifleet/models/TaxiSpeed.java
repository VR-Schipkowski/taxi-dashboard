package com.taxifleet.models;

//todo: maybe use time instead of strings, then we probably dont have to parse them all over again
public class TaxiSpeed extends TaxiLocation {
    public double speed;
    public double averageSpeed;
    public double totalDistance;
    public double curDistance;
    public boolean isSpeeding;
    public boolean isOutOfArea;
    public String lastMoved;
    public boolean isParking;
    public boolean speedingStateChanged;

    public TaxiSpeed() {
        super();
    }

    public TaxiSpeed(TaxiLocation loc) {
        this.taxi_id = loc.taxi_id;
        this.timestamp = loc.timestamp;
        this.eventTimeMillis = loc.eventTimeMillis;
        this.longitude = loc.longitude;
        this.latitude = loc.latitude;
        this.ingested_at = loc.ingested_at;
    }

    @Override
    public String toString() {
        String r = String.format("Taxi %d: %.2f km/h at (%.5f, %.5f)",
                taxi_id, speed, latitude, longitude);
        if (isSpeeding)
            r += " !!!Speeding!!!";
        if (isOutOfArea)
            r += " !!!OutOfArea!!!";
        return r;
    }
}
