package com.learnflink.split;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitByFilter {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> lineDs = env.readTextFile("C:\\Users\\dynam\\IdeaProjects\\learnFlink\\src\\main\\resources\\number.txt");

        lineDs.filter(value -> Integer.parseInt(value) % 2 == 0).print("Even Number Stream");


        lineDs.filter(value -> Integer.parseInt(value) % 2 == 1).print("Odd Number Stream");


        env.execute();
    }
}
