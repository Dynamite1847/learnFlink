package com.learnflink.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.protocol.types.Field;

public class CustomPartitionDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> lineDs = env.readTextFile("C:\\Users\\dynam\\IdeaProjects\\learnFlink\\src\\main\\resources\\number.txt");

        lineDs.partitionCustom(new MyPartitioner(), s -> s).print();

        env.execute();
    }
}
