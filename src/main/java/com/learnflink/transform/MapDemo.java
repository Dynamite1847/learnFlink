package com.learnflink.transform;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1));
        //MAP算子，一进一出
        //方式一：匿名实现类
//        SingleOutputStreamOperator<String> map = waterSensorDataStreamSource.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        });

        //方法二：Lambda Expression
//        SingleOutputStreamOperator<String> map = waterSensorDataStreamSource.map(waterSensor -> waterSensor.getId());

        //方法三：定义一个类来实现MapFunction
        SingleOutputStreamOperator<String> map = waterSensorDataStreamSource.map(new MyMapFunction());
        map.print();

        env.execute();
    }


    public  static class MyMapFunction implements MapFunction<WaterSensor, String>{

        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.getId();
        }
    }
}
