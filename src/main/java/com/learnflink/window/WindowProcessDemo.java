package com.learnflink.window;

import com.learnflink.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStream.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWindowedStream = waterSensorKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = waterSensorWindowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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


        });

        process.print();

        env.execute();
    }
}
