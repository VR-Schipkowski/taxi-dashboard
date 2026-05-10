package com.taxifleet.helper;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Helper {

    public final static double EARTH_RADIUS = 6378.137;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static double haversine(double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }

    public static double calculateDistance(double startLat, double startLong, double endLat, double endLong) {

        double dLat = Math.toRadians((endLat - startLat));
        double dLong = Math.toRadians((endLong - startLong));

        startLat = Math.toRadians(startLat);
        endLat = Math.toRadians(endLat);

        double a = haversine(dLat) + Math.cos(startLat) * Math.cos(endLat) * haversine(dLong);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c;
    }

    public static long parseTimestamp(String timestamp) {
        try {
            Date date = dateFormat.parse(timestamp);
            return date.getTime();
        } catch (Exception e) {
            System.err.println("Failed to parse timestamp: " + timestamp);
            return 0;
        }
    }

    public static double calculateTimeDifferenceHours(String timestamp1, String timestamp2) {
        long time1Ms = parseTimestamp(timestamp1);
        long time2Ms = parseTimestamp(timestamp2);
        long diffMs = Math.abs(time2Ms - time1Ms);
        return diffMs / (1000.0 * 60.0 * 60.0);
    }
}
