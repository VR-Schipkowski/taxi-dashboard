package com.taxifleet.functions.heatmap;

import com.taxifleet.models.TaxiSpeed;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

public class DistinctTaxiCountAggregator
        implements AggregateFunction<TaxiSpeed, Set<Integer>, Integer> {

    @Override
    public Set<Integer> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<Integer> add(TaxiSpeed location, Set<Integer> accumulator) {
        accumulator.add(location.taxi_id);
        return accumulator;
    }

    @Override
    public Integer getResult(Set<Integer> accumulator) {
        return accumulator.size();
    }

    @Override
    public Set<Integer> merge(Set<Integer> a, Set<Integer> b) {
        a.addAll(b);
        return a;
    }
}