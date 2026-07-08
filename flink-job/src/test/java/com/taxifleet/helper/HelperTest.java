package com.taxifleet.helper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Helper} — the Haversine distance and timestamp maths
 * used by the speed and distance calculations across the Flink job.
 */
class HelperTest {

    private static final double EPS = 0.01; // km / hour tolerance

    @Test
    void distanceBetweenSamePointIsZero() {
        assertEquals(0.0, Helper.calculateDistance(39.9, 116.4, 39.9, 116.4), 1e-9);
    }

    @Test
    void distanceIsSymmetric() {
        double ab = Helper.calculateDistance(39.90, 116.40, 39.95, 116.45);
        double ba = Helper.calculateDistance(39.95, 116.45, 39.90, 116.40);
        assertEquals(ab, ba, 1e-9);
    }

    @Test
    void oneDegreeLatitudeIsAboutOneHundredElevenKm() {
        // One degree of latitude is ~111 km anywhere on Earth.
        double d = Helper.calculateDistance(39.0, 116.0, 40.0, 116.0);
        assertEquals(111.0, d, 1.0);
    }

    @Test
    void knownDistanceBeijingToShanghai() {
        // Beijing (~39.90, 116.40) to Shanghai (~31.23, 121.47) is ~1060-1070 km.
        double d = Helper.calculateDistance(39.9042, 116.4074, 31.2304, 121.4737);
        assertTrue(d > 1000 && d < 1100,
                "expected ~1060 km, got " + d);
    }

    @Test
    void timeDifferenceHoursIsCorrect() {
        double hours = Helper.calculateTimeDifferenceHours(
                "2008-02-02 12:00:00", "2008-02-02 13:30:00");
        assertEquals(1.5, hours, EPS);
    }

    @Test
    void timeDifferenceSecondsIsCorrect() {
        double seconds = Helper.calculateTimeDifferenceSeconds(
                "2008-02-02 12:00:00", "2008-02-02 12:00:45");
        assertEquals(45.0, seconds, EPS);
    }

    @Test
    void timeDifferenceIsAbsolute() {
        // Order should not matter — the helper returns an absolute difference.
        double forward = Helper.calculateTimeDifferenceSeconds(
                "2008-02-02 12:00:00", "2008-02-02 12:01:00");
        double backward = Helper.calculateTimeDifferenceSeconds(
                "2008-02-02 12:01:00", "2008-02-02 12:00:00");
        assertEquals(forward, backward, 1e-9);
        assertEquals(60.0, forward, EPS);
    }

    @Test
    void parseTimestampReturnsEpochMillis() {
        long ms = Helper.parseTimestamp("2008-02-02 12:00:00");
        assertTrue(ms > 0, "valid timestamp should parse to a positive epoch millis");
    }

    @Test
    void invalidTimestampReturnsZero() {
        assertEquals(0L, Helper.parseTimestamp("not-a-date"));
    }
}
