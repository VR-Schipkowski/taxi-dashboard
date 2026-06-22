package com.taxifleet.functions;

import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.util.OutputTag;

public class OutOfAreaProcess {

    public static final OutputTag<TaxiSpeed> OUT_OF_AREA_TAG = new OutputTag<TaxiSpeed>("out_of_area") {
    };
}