package com.learnflink.watermark;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WaterMarkMonotonousDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = env.socketTextStream("192.168.31.185", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                System.out.println("WaterSensor= " + waterSensor + " ,recordTs= " + l);
                return waterSensor.getTs() * 1000L;
            }
        });


        waterSensorDataStream.
                assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .keyBy(sensor -> sensor.getId())
                //使用事件时间语义的窗口， watermark才能启动
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * @param s 分组的key
                     * @param context 上下文
                     * @param iterable 存的数据
                     * @param collector 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = iterable.spliterator().estimateSize();

                        collector.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]，包含" + count + "条数据." + iterable.toString());

                    }


                }).print();

        env.execute();
    }
}
