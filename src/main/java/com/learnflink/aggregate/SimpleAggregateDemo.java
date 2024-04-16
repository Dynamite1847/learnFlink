package com.learnflink.aggregate;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1),
                new WaterSensor("s1",2L,10)
                );

        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });

        /**
         * 简单聚合算子
         * 1. 在keyby之后才能调用
         * 2. 位置索引适用于tuple类型
         */

        SingleOutputStreamOperator<WaterSensor> sum = waterSensorKeyedStream.sum("vc");

        sum.print();
        env.execute();
    }
}
