package com.learnflink.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<Integer> source2 = env.fromElements(1, 1, 2, 2, 3, 3);

        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");

        ConnectedStreams<Integer, String> connect = source1.connect(source3);

        /**
         * 一次只能链接两条流
         * 流的数据类型可以不同
         * 连接后可以用map flatmap process处理，但是是各处理各的
         */
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute();
    }
}
