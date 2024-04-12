package com.learnflink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据:从文件读取
        DataStreamSource<String> lineDs = env.readTextFile("C:\\Users\\dynam\\IdeaProjects\\learnFlink\\src\\main\\resources\\word.txt");

        //切分，转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> output) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);

                    output.collect(wordTuple2);
                }
            }
        });
        //按照word分组
        KeyedStream<Tuple2<String, Integer>, String> wordCountKeyedStream = wordAndCount.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        //各分组内聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordCountKeyedStream.sum(1);

        //输出
        sum.print();

        //执行
        env.execute();
    }
}
