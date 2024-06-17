package com.learnflink.state;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class KeyedListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataSet = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTs() * 1000L));


        waterSensorDataSet.keyBy(waterSensor -> waterSensor.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ListState<Integer> vcListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState", Types.INT));
            }

            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                //来一条，存到list状态里
                vcListState.add(waterSensor.getVc());
                //从list状态拿出来，排序，只留三个最大的
                Iterable<Integer> vcIterable = vcListState.get();
                ArrayList<Integer> vcList = new ArrayList<>();
                for (Integer vc : vcIterable) {
                    vcList.add(vc);
                }
                vcList.sort((o1, o2) -> o2 - o1);
                //更新list状态
                if (vcList.size() > 3) {
                    vcList.remove(3);
                }

                collector.collect("Sensor id:" + waterSensor.getId() + ", top 3 Vc are:" + vcList.toString());

                vcListState.update(vcList);
            }
        }).print();
        env.execute();
    }
}
