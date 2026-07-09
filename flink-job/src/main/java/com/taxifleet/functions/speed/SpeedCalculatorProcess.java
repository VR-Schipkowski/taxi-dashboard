package com.taxifleet.functions.speed;

import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.util.OutputTag;

public class SpeedCalculatorProcess {

    public static final OutputTag<TaxiSpeed> SPEEDING_TAG = new OutputTag<TaxiSpeed>("speeding") {
    };
}