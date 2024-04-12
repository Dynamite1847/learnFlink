package com.learnflink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据:从文件读取
        DataSource<String> lineDS = env.readTextFile("C:\\Users\\dynam\\IdeaProjects\\learnFlink\\src\\main\\resources\\word.txt");

        //切分，转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndCount = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
        UnsortedGrouping<Tuple2<String, Integer>> wordCountgroup = wordAndCount.groupBy(0);
        //各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordCountgroup.sum(1);

        //输出
        sum.print();

    }
}
