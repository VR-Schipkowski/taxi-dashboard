package com.taxifleet.models;

public class HeatmapCell {
    public String cellId;
    public int taxiCount;
    public long windowStart;
    public long windowEnd;

    public HeatmapCell() {}

    public HeatmapCell(String cellId, int taxiCount, long windowStart, long windowEnd) {
        this.cellId = cellId;
        this.taxiCount = taxiCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
}