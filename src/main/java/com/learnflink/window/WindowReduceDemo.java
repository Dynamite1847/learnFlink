package com.learnflink.window;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1),
                new WaterSensor("s1",1L,3));


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStreamSource.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWindowedStream = waterSensorKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)));

        /**
         *  窗口的reduce：
         *  增量聚合：来一条数据，就会计算一次，但是不会输出
         *  在窗口触发的时侯，才会输出窗口的最终计算结果
         */
        SingleOutputStreamOperator<WaterSensor> reduce = waterSensorWindowedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor waterSensor1) throws Exception {
                System.out.println("调用reduce方法,value1= " + waterSensor + " value2= " + waterSensor1);
                return new WaterSensor(waterSensor.getId(), waterSensor.getTs(), waterSensor.getVc() + waterSensor1.getVc());
            }
        });

        reduce.print();

        env.execute();
    }
}
