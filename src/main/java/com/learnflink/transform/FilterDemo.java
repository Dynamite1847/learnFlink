package com.learnflink.transform;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1));

        //为true就返回，为false就过滤
        SingleOutputStreamOperator<WaterSensor> filter = waterSensorDataStreamSource.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return "s1".equals(waterSensor.getId());
            }
        });


        filter.print();

        env.execute();
    }


    public  static class MyMapFunction implements MapFunction<WaterSensor, String>{

        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.getId();
        }
    }
}
