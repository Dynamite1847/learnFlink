package com.learnflink.state;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

public class KeyedMapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataSet = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTs() * 1000L));


        waterSensorDataSet.keyBy(waterSensor -> waterSensor.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {

            MapState<Integer,Integer> vcCountMapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                vcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("vcCountMapState",Types.INT,Types.INT));
            }

            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                //判断是否存在vc对应的key
                Integer vc = waterSensor.getVc();
                if (vcCountMapState.contains(vc)) {
                    Integer count = vcCountMapState.get(vc);
                    vcCountMapState.put(vc,++count);
                }else {
                    vcCountMapState.put(vc,1);
                }
                StringBuilder outString = new StringBuilder();

                outString.append("Sensor id:" + waterSensor.getId()+"\n");
                for (Map.Entry<Integer,Integer> vcCount: vcCountMapState.entries()){
                    outString.append(vcCount.toString()+"\n");
                }

                collector.collect(outString.toString());
            }
        }).print();
        env.execute();
    }
}
