package com.learnflink.state;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedStateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataSet = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTs() * 1000L));


        waterSensorDataSet.keyBy(waterSensor -> waterSensor.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ValueState<Integer> lastVcState;

            //只能在open方法中初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //创建stateTTLConfig
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();

                //启用TTL
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                lastVcState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                //取出上一条数据的水位值
                int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                collector.collect("key=" + waterSensor.getId() + "last Vc=" + lastVc);

                lastVcState.update(waterSensor.getVc());
            }
        }).print();
        env.execute();
    }
}
