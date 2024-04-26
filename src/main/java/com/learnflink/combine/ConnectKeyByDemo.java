package com.learnflink.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //此处更改并行度会影响结果
        env.setParallelism(1);
        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );

        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        //多并行度下，需要根据关联条件进行keyby，才能在多并行度下看到完整结果
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKeyBy = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);
        /**
         * 实现互相匹配的效果，
         * 两条流不一定谁的数据先来
         * 每条流有数据来，就存到一个变量中：hashmap<id,list<value>>
         * 每条流有数据来的时候，除了存变量中，去另一条流存在变量查找是否有匹配上的
         */
        SingleOutputStreamOperator<String> process = connectKeyBy.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            /**
             *
             * @param value The stream element
             * @param ctx A
             * @param out The collector to emit resulting elements to
             * @throws Exception
             */
            Map<Integer, List<Tuple2<Integer, String>>> s1Map = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Map = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                if (!s1Map.containsKey(id)) {
                    ArrayList<Tuple2<Integer, String>> s1List = new ArrayList<>();
                    s1List.add(value);
                    s1Map.put(id, s1List);
                } else {
                    s1Map.get(id).add(value);
                }
                //判断是否有匹配到的key，匹配到就输出，匹配不到就不输出
                if (s2Map.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> element : s2Map.get(id)) {
                        out.collect("s1: " + value + "<====>" + "s2:" + element);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                if (!s2Map.containsKey(id)) {
                    ArrayList<Tuple3<Integer, String, Integer>> s2List = new ArrayList<>();
                    s2List.add(value);
                    s2Map.put(id, s2List);
                } else {
                    s2Map.get(id).add(value);
                }
                //判断是否有匹配到的key，匹配到就输出，匹配不到就不输出
                if (s1Map.containsKey(id)) {
                    for (Tuple2<Integer, String> element : s1Map.get(id)) {
                        out.collect("s1: " + element + "<====>" + "s2:" + value);
                    }
                }
            }
        });

        process.print();

        env.execute();
    }
}
