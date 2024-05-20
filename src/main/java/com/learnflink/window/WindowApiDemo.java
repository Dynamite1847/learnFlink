package com.learnflink.window;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1));


        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDataStreamSource.keyBy(waterSensor -> waterSensor.getId());

        /**
         * 1. 指定窗口分配器： 指定用哪一种窗口（时间 or 计数）
         * 没有keyby的窗口，窗口内的所有数据进入同一个子任务，并行度只能为1
         * 有keyby的窗口，每个key上都定义了一组窗口，各自独立地进行统计计算
         */


        //基于时间的
        waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10))); //滚动窗口，窗口长度10s

        waterSensorStringKeyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))); //滑动窗口，窗口长度10s，滑动步长2s

        waterSensorStringKeyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); //会话窗口，超时间隔5s

        //基于计数的
        WindowedStream<WaterSensor, String, GlobalWindow> waterSensorwWindowedStream = waterSensorStringKeyedStream.countWindow(5);// 滚动窗口，窗口长度5个元素

        waterSensorStringKeyedStream.countWindow(5,2); //滑动窗口，窗口长度5个元素，滑动步长2个元素

        waterSensorStringKeyedStream.window(GlobalWindows.create()); //全局窗口，计数窗口的底层，需要自定义触发器


//        waterSensorStringKeyedStream.windowAll();

        /**
         * 2. 指定窗口函数： 窗口内数据的计算逻辑
         * 增量聚合：来一条数据，计算一条数据，窗口出发的时候输出计算结果
         *
         * 全窗口函数：数据来了不计算，等触发一起计算
         */




        env.execute();
    }
}
