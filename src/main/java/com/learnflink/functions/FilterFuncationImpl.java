package com.learnflink.functions;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterFuncationImpl implements FilterFunction<WaterSensor> {
    public String id;

    public FilterFuncationImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor waterSensor) throws Exception {
        return this.id.equals(waterSensor.getId());
    }
}
