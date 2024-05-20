package com.learnflink.window;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1),
                new WaterSensor("s1", 1L, 3));


        KeyedStream<WaterSensor, String> waterSensorKeyedStream = waterSensorDataStreamSource.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> waterSensorWindowedStream = waterSensorKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)));

        /**
         * 窗口函数：增量聚合Aggregate
         *
         * 1. 属于本窗口的第一条数据来的时候，创建窗口，创建累加器
         * 2. 增量聚合：来一条计算一条，调用一次add方法
         * 3. 窗口输出时调用一次getresult方法
         * 4. 输入，中间累加器，输出类型可以不一样，非常灵活
         */
        SingleOutputStreamOperator<String> accumulator = waterSensorWindowedStream.aggregate(

                /**
                 *第一个类型：输入路径的类型
                 * 第二个类型：累加器的类型，存储的中间计算结果的类型
                 * 第三个类型：输出的类型
                 */

                new AggregateFunction<WaterSensor, Integer, String>() {
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
                });

        accumulator.print();
        env.execute();
    }
}
