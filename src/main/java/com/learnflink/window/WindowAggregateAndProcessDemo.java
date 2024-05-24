package com.learnflink.window;

import com.learnflink.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStream.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWindowedStream = waterSensorKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        /**
         * 增量聚合aggregate+全窗口process
         * 1. 增量聚合函数处理数据：来一条计算一条
         * 2. 窗口触发时，增量聚合的结果（只有一条）传递给全窗口函数
         * 3. 经过全窗口函数的处理包装后，输出
         *
         * 结合两者优点：
         * 1. 增量聚合：来一条算一条，存储中间的计算结果，占用的空间少
         * 2. 全窗口函数：可以通过上下文实现灵活动能
         */
        SingleOutputStreamOperator<String> result = waterSensorWindowedStream.aggregate(new MyAgg(), new MyProcess());

        result.print();

        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {

        @Override
        public Integer createAccumulator() {
            System.out.println("Initialize Accumulator");
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            return integer + waterSensor.getVc();
        }

        @Override
        public String getResult(Integer integer) {
            return integer.toString();
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            //只有会话窗口会用到
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String,TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();
            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = iterable.spliterator().estimateSize();

            collector.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]，包含" + count + "条数据." + iterable.toString());

        }
    }
}
