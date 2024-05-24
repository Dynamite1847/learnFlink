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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStream.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWindowedStream = waterSensorKeyedStream
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)));//滑动窗口，长度10s，步长5s
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));//会话窗口，间隔5s
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
                    @Override
                    public long extract(WaterSensor waterSensor) {
                        //从数据中提取ts，作为间隔，单位毫秒
                        return waterSensor.getTs()*1000L;
                    }
                }));

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
