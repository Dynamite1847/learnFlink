package com.learnflink.aggregate;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
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
         * reduce:
         * keyby之后调用
         * 输入类型=输出类型，类型不能变
         * 每个key的第一条数据来的时候不会执行reduce方法，存起来直接输出
         * reduce中的两个参数：
         * value1：之前的计算结果，存状态
         * value2：现在来的数据
         */

        SingleOutputStreamOperator<WaterSensor> reduce = waterSensorKeyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor1, WaterSensor waterSensor2) throws Exception {
                System.out.println("water sensor1=" + waterSensor1);
                System.out.println("water sensor2=" + waterSensor2);
                return new WaterSensor(waterSensor1.id, waterSensor2.ts, waterSensor1.vc + waterSensor2.vc);
            }
        });

        reduce.print();

        env.execute();
    }
}
