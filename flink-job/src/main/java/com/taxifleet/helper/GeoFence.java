package com.taxifleet.helper;

/*
 * GeoFence for the taxi operating area.
 *
 * The T-Drive dataset tracks taxis in Beijing. "Out of area" is defined as a
 * position more than MAX_DISTANCE_FROM_CITY_KM from the Forbidden City (the
 * city centre). This is the SAME definition used by OutOfAreaProcessFunction,
 * so the isOutOfArea flag set in the TaxiSpeed constructor is consistent with
 * the alert side-output further down the pipeline.
 *
 * Keeping the rule here (and matching it to OutOfAreaProcessFunction) means the
 * isOutOfArea field is correct everywhere it is read: Redis, the taxi-processed
 * topic, and the dashboard payload.
 */
public final class GeoFence {

    // City centre reference point (Forbidden City) — must match
    // OutOfAreaProcessFunction.
    public static final double FORBIDDEN_CITY_LATITUDE = 39.9163;
    public static final double FORBIDDEN_CITY_LONGITUDE = 116.3972;

    // A taxi further than this from the city centre is "out of area".
    public static final double MAX_DISTANCE_FROM_CITY_KM = 15.0;

    private GeoFence() {
        // utility class
    }

    /**
     * Returns true when (latitude, longitude) is more than
     * MAX_DISTANCE_FROM_CITY_KM from the city centre.
     */
    public static boolean isOutOfArea(double latitude, double longitude) {
        double distanceKm = Helper.calculateDistance(
                latitude, longitude,
                FORBIDDEN_CITY_LATITUDE, FORBIDDEN_CITY_LONGITUDE);
        return distanceKm > MAX_DISTANCE_FROM_CITY_KM;
    }
}
