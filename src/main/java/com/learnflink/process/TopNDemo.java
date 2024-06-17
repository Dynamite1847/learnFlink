package com.learnflink.process;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataSet = env.socketTextStream("192.168.31.185", 7777)
                .map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTs() * 1000L));

        waterSensorDataSet.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new MyTopNProcessAllWindowFunction()).print();


        //思路1： 所有数据到一起，用hashmap才存

        env.execute();

    }

    public static class MyTopNProcessAllWindowFunction extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow>{

        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
            //定义一个hashmap用来存，key=vc，value=count

            Map<Integer, Integer> vcCountMap = new HashMap<>();
            //1.遍历数据
            for (WaterSensor element: iterable){
                Integer vc = element.getVc();
                //1.1如果不是第一条数据
                if (vcCountMap.containsKey(element.getVc())){
                    vcCountMap.put(vc,vcCountMap.get(vc)+1 );
                }else {
                    //1.2如果是第一条数据
                    vcCountMap.put(vc,1);
                }

            }

            //2. 对count进行排序
            ArrayList<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                datas.add(Tuple2.of(vc,vcCountMap.get(vc)));
            }


            datas.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                    return o2.f1-o1.f1;
                }
            });

            //3. 取出count最大的两个vc

            StringBuilder outString = new StringBuilder();

            outString.append("==================");
            for (int i = 0; i <Math.min(datas.size(),2) ; i++) {
                outString.append("No."+ i+1);
                outString.append("\n");
                Tuple2<Integer, Integer> vcCount = datas.get(i);
                outString.append("vc="+ vcCount.f0);
                outString.append("\n");
                outString.append(",count="+ vcCount.f1);
                outString.append("\n");
                outString.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd hh:mm:ss.SSS"));
                outString.append("\n");
                outString.append("==================");
            }

            collector.collect(outString.toString());
        }
    }
}
