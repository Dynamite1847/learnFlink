package com.learnflink.process;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = env.socketTextStream("192.168.31.185", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTs() * 1000L);


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStream.
                assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .keyBy(sensor -> sensor.getId());
        //TODO: Process: keyed

        /**
         * 定时器
         * 1.只有keyed才有
         * 2. 事件时间定时器，通过watermark触发的
         *     watermark>= 注册的时间
         *     watermark = 当前最大事件时间 - 等待时间 -1ms， 因为-1ms， 会推迟一条数据
         * 3. 在process中获取watermark， 获得的是上次生成的watermark，因为process中还没接收到这条数据对应生成的新watermark
         *
         */
        waterSensorKeyedStream.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    /**
                     * 来一条数据处理一次，
                     * @param waterSensor
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        String currentKey = context.getCurrentKey();

                        //定时器
                        TimerService timerService = context.timerService();
                        //注册定时器：事件时间
                        long currentTimeStamp = timerService.currentProcessingTime();

                        timerService.registerProcessingTimeTimer(currentTimeStamp + 5000L);

                        System.out.println("Current Key is：" + currentKey + " , Current Event Time is: " + currentTimeStamp + ", Registered a timer with 5 seconds.");
                    }

                    /**
                     * 时间进展到定时器注册的时间就会调用该方法
                     * @param timestamp 当前时间进展
                     * @param ctx 上下文
                     * @param out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        System.out.println("现在时间是" + timestamp + "定时器触发");
                    }
                }

        );

        env.execute();
    }
}
