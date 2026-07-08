package com.taxifleet.helper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link GeoFence}, the out-of-area rule used across the pipeline
 * (TaxiSpeed constructor, OutOfAreaProcessFunction, the taxi-processed payload).
 *
 * The rule: a taxi is "out of area" when it is more than
 * MAX_DISTANCE_FROM_CITY_KM (15 km) from the Forbidden City.
 */
class GeoFenceTest {

    private static final double CENTER_LAT = GeoFence.FORBIDDEN_CITY_LATITUDE;
    private static final double CENTER_LON = GeoFence.FORBIDDEN_CITY_LONGITUDE;

    @Test
    void cityCentreIsInsideArea() {
        // The reference point itself is 0 km away — always in area.
        assertFalse(GeoFence.isOutOfArea(CENTER_LAT, CENTER_LON));
    }

    @Test
    void pointWellInsideRadiusIsInArea() {
        // ~5 km north of centre — comfortably inside the 15 km radius.
        assertFalse(GeoFence.isOutOfArea(39.9613, CENTER_LON));
    }

    @Test
    void pointFarNorthIsOutOfArea() {
        // ~16 km north of centre (this is the real value taxi 16000 hit in
        // manual testing) — must be flagged out of area.
        assertTrue(GeoFence.isOutOfArea(40.0595, CENTER_LON));
    }

    @Test
    void pointFarEastIsOutOfArea() {
        // Push well east of the city — clearly outside.
        assertTrue(GeoFence.isOutOfArea(CENTER_LAT, 116.70));
    }

    @Test
    void distantCityIsOutOfArea() {
        // Shanghai — obviously out of area.
        assertTrue(GeoFence.isOutOfArea(31.2304, 121.4737));
    }

    @Test
    void boundaryIsConsistentWithComputedDistance() {
        // Cross-check the fence against the underlying Haversine distance:
        // isOutOfArea must be true exactly when distance > threshold.
        double lat = 40.02;
        double lon = 116.55;
        double distance = Helper.calculateDistance(
                lat, lon, CENTER_LAT, CENTER_LON);
        boolean expected = distance > GeoFence.MAX_DISTANCE_FROM_CITY_KM;
        assertEquals(expected, GeoFence.isOutOfArea(lat, lon));
    }
}
