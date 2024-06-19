package com.learnflink.state;

import com.learnflink.bean.WaterSensor;
import com.learnflink.window.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        //数据流
        SingleOutputStreamOperator<WaterSensor> waterSensorDataSet = env.socketTextStream("192.168.31.185", 7777).map(new WaterSensorMapFunction());

        //配置流，用来广播配置
        DataStreamSource<String> thresholdDataSet = env.socketTextStream("192.168.31.185", 8888);

        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> thresholdBroadcastStream
                = thresholdDataSet.broadcast(broadcastMapState);

        BroadcastConnectedStream<WaterSensor, String> waterSensorBCS = waterSensorDataSet.connect(thresholdBroadcastStream);

        SingleOutputStreamOperator<String> process = waterSensorBCS.process(
                new BroadcastProcessFunction<WaterSensor, String, String>() {
                    /**
                     * 数据流的处理方法：数据流只能读取广播状态，不能修改
                     * @param waterSensor
                     * @param readOnlyContext
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor waterSensor, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        //通过获取上下文获取广播状态，取出里面的值（只读，不能修改）
                        ReadOnlyBroadcastState<String, Integer> readOnlyBroadcastState = readOnlyContext.getBroadcastState(broadcastMapState);
                        //刚启动时可能是数据流的第一条数据先来
                        Integer threshold = readOnlyBroadcastState.get("threshold") ==null?0:readOnlyBroadcastState.get("threshold");
                        if (waterSensor.getVc() > threshold) {
                            collector.collect("Sensor warning! Vc is greater than " + threshold + "!!");
                        }

                    }

                    /**
                     * 广播后的配置流的处理方法：只有广播流才能修改广播状态
                     * @param s
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(String s, BroadcastProcessFunction<WaterSensor, String, String>.Context context, Collector<String> collector) throws Exception {
                        //通过上下文获取广播状态,往里写数据
                        BroadcastState<String, Integer> broadcastState = context.getBroadcastState(broadcastMapState);
                        broadcastState.put("threshold", Integer.valueOf(s));
                    }
                }
        );
        env.execute();
    }
}
