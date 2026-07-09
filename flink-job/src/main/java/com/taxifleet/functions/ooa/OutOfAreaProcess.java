package com.taxifleet.functions.ooa;

import com.taxifleet.models.TaxiLocation;
import org.apache.flink.util.OutputTag;

public class OutOfAreaProcess {

    public static final OutputTag<TaxiLocation> OUT_OF_AREA_TAG = new OutputTag<TaxiLocation>("out_of_area") {
    };
    public static final OutputTag<TaxiLocation> OOA_RETURNED_TAG = new OutputTag<TaxiLocation>("ooa-returned"){
    };
}