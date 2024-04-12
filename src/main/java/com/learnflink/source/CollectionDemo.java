package com.learnflink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合读取数据
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4));

        //直接填写元素
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);

        source.print();

        env.execute();
    }
}
