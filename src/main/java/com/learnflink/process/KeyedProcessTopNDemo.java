package com.learnflink.process;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class KeyedProcessTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataSet = env.socketTextStream("192.168.31.185", 7777)
                .map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTs() * 1000L));


        //思路2:做keyby，使用KeyedProcessFunction实现
        /**
         * 1. 按照vc做keyby, 开窗，分别count
         *      增量聚合，计算count
         *      全窗口，对计算结果count值封装，带上窗口结束时间的标签
         *          为了让同一个窗口的时间范围的计算结果到一起去
         * 2. 对同一个窗口范围的count值进行处理： 排序，取前n个
         *      按照windowEnd做keyby
         *      使用process，来一条调用一次，需要先存，分开存HashMap， key= windowEnd, value=list
         *          使用定时器，对存起来的结果进行排序，取前n个
         *
         */
        //开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口标记windowEnd
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAggregate = waterSensorDataSet.keyBy(waterSensor -> waterSensor.getVc()).
                window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new VcCountAgg(), new WindowResult());


        windowAggregate.keyBy(r -> r.f2)
                .process(new TopN(2))
                .print();

        env.execute();

    }

    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return null;
        }
    }

    /**
     * 泛型如下：
     * 1. 输入类型=增量函数的输出 count值， Integer
     * 2. 输出类型 = Tuple3(vc,count,windowEnd), 带上窗口结束时间的标签
     * 3. key类型 = vc, Integer
     * 4. 窗口类型
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {

        @Override
        public void process(Integer vc, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
            //迭代器里面只有一条数据，next一次即可
            Integer count = iterable.iterator().next();
            long windowEnd = context.window().getEnd();
            collector.collect(Tuple3.of(vc, count, windowEnd));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {

        //存不同窗口的统计结果，key = windowend，value=list
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;

        //要取的n的数量
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context context, Collector<String> collector) throws Exception {
            //进入这个方法的只是一条数据，想要排序，得到齐才行，不同窗口要分开存
            //1.存到HashMap中
            Long windowEnd = value.f2;

            if (dataListMap.containsKey(windowEnd)) {
                //1.1不是vc的第一条，直接加入list中
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(value);
            } else {
                //不包含vc，是第一条，初始化list添加到list中
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataListMap.put(windowEnd, dataList);
            }

            //2.注册定时器，windowEnd+1ms即可（同一个窗口范围，应该同时输出，只不过是一条一条调用processElement方法，只需要延迟1ms）
            context.timerService().registerEventTimeTimer(windowEnd + 1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，同一个窗口的计算结果攒齐了，开始排序
            //1.排序
            Long windowEnd = ctx.getCurrentKey();
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            //2.取topN

            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1 - o1.f1;
                }
            });

            StringBuilder outString = new StringBuilder();

            outString.append("==================");
            for (int i = 0; i < Math.min(dataList.size(), threshold); i++) {
                outString.append("No." + i + 1);
                outString.append("\n");
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outString.append("vc=" + vcCount.f0);
                outString.append("\n");
                outString.append(",count=" + vcCount.f1);
                outString.append("\n");
                outString.append("窗口结束时间=" + vcCount.f2);
                outString.append("\n");
                outString.append("==================");
            }
            //用完的list,及时清理，节省资源
            dataList.clear();

            out.collect(outString.toString());
        }
    }

}
