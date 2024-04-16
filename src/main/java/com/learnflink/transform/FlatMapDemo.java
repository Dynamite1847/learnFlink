package com.learnflink.transform;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1));

        //如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc。
        SingleOutputStreamOperator<String> flatmap = waterSensorDataStreamSource.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                if ("s1".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getVc().toString());
                } else if ("s2".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getVc().toString());
                    collector.collect(waterSensor.getTs().toString());
                }
            }
        });

        flatmap.print();

        env.execute();
    }

}
