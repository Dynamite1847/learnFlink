package com.learnflink.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<Integer> source2 = env.fromElements(1, 1, 2, 2, 3, 3);

        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");

        /**
         * 合并数据流：
         * 1. 数据类型必须一致
         * 2.一次可以多条
         */
        DataStream<Integer> union = source1.union(source2).union(source3.map(value -> Integer.parseInt(value)));

        union.print();


        env.execute();
    }
}
