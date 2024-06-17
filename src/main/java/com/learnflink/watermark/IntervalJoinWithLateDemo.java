package com.learnflink.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("192.168.31.185", 7777)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] data = s.split(",");
                        return Tuple2.of(data[0], Integer.valueOf(data[1]));

                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String,Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value,ts)->value.f1*1000L)
                );


        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> dataStream1 = env.socketTextStream("192.168.31.185", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer,Integer>>() {
                    @Override
                    public Tuple3<String, Integer,Integer> map(String s) throws Exception {
                        String[] data = s.split(",");
                        return Tuple3.of(data[0], Integer.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String,Integer,Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value,ts)-> value.f1*1000L));

        /**
         * Interval Join
         * 1. 只支持事件时间
         * 2. 指定上界，下界的偏移
         * 3. process中，只能处理join上的数据
         * 4. 两条流关联后的watermark，以两条流中最小的为准
         * 5. 如果 当前数据的事件时间 < 当前的watermark，就是迟到数据，主流process不处理
         * => between后，指定将左流或右流迟到数据放入侧输出流
         */

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = dataStream.keyBy(ds1 -> ds1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> keyedStream1 = dataStream1.keyBy(ds2 -> ds2.f0);

        OutputTag<Tuple2<String, Integer>> ks1LateTag = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> ks2LateTag = new OutputTag<>("ks2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        SingleOutputStreamOperator<String> process = keyedStream.intervalJoin(keyedStream1)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(ks1LateTag)
                .sideOutputRightLateData(ks2LateTag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 两条流的数据都匹配上才会调用这个方法
                     * @param stringIntegerTuple2
                     * @param stringIntegerIntegerTuple3
                     * @param context 上下文
                     * @param collector 采集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(stringIntegerTuple2 + "<--->" + stringIntegerIntegerTuple3);
                    }
                });


        process.print("Main Stream");

        process.getSideOutput(ks1LateTag).printToErr("Late from ks1");
        process.getSideOutput(ks2LateTag).printToErr("Late from ks2");
        
        env.execute();
    }
}
