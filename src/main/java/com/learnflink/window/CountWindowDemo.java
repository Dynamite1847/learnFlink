package com.learnflink.window;

import com.learnflink.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStream.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, GlobalWindow> waterSensorWindowedStream = waterSensorKeyedStream.countWindow(5,2);//每经过一个步长，就有一个窗口触发输出，第一次输出在第二条数据来的时候


        SingleOutputStreamOperator<String> process = waterSensorWindowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                long maxTs = context.window().maxTimestamp();
                String maxTime = DateFormatUtils.format(maxTs, "yyyy-MM-dd HH:mm:ss.SSS");
                long count = iterable.spliterator().estimateSize();
                collector.collect("key=" + s + "的窗口[" + maxTime + "]，包含" + count + "条数据." + iterable.toString());
            }
        });

        process.print();

        env.execute();
    }
}
