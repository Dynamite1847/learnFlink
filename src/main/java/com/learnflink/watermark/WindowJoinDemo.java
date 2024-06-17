package com.learnflink.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream1 = env.fromElements(Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("c", 4)).assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> dataStream2 = env.fromElements(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 11, 1),
                Tuple3.of("b", 2, 1),
                Tuple3.of("c", 14, 1),
                Tuple3.of("d", 15, 1)).assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        /**
         * window join
         * 1. 落在同一个时间窗口范围内才能匹配
         * 2. 根据keyby的key来进行关联匹配
         * 3. 只能拿到匹配上的数据，类似有时间范围的inner join
         */

        DataStream<String> join = dataStream1.join(dataStream2).where(ds1 -> ds1.f0).equalTo(ds2 -> ds2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 关联上的数据会调用join方法
                     * @param stringIntegerTuple2 ds1数据
                     * @param stringIntegerIntegerTuple3 ds2数据
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public String join(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) throws Exception {
                        return stringIntegerTuple2 + "<---->" + stringIntegerIntegerTuple3;
                    }
                });

        join.print();
        env.execute();
    }
}

