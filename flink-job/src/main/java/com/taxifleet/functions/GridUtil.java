package com.taxifleet.functions;

public class GridUtil {
    // Cell size in degrees. ~0.01° latitude ≈ 1.1 km — tune to your area's scale.
    // you also need to change it in heatmap_helper.js in frontend
    private static final double CELL_SIZE = 0.015;

    public static String cellFor(double latitude, double longitude) {
        long latCell = Math.floorDiv((long) (latitude * 1000), (long) (CELL_SIZE * 1000));
        long lonCell = Math.floorDiv((long) (longitude * 1000), (long) (CELL_SIZE * 1000));
        return latCell + "_" + lonCell;
    }
}