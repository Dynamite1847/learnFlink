package com.learnflink.aggregate;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class keybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1));

        /**
         * 1. 返回是一个KeyedStream，键控流
         * 2. keyby不是转换算子，只是对数据进行重分区，不能设置并行度
         * 3. keyby分组与分区的关系：
         *  keyby对数据分组，保证相同key的数据在同一分区
         *  分区：一个子任务算一个分区，一个分区中可以存在多个分组
         *  分组：相同key的是一个分组
         *
         */
        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });


        env.execute();
    }
}
