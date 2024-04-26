package com.learnflink.split;

import com.learnflink.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    /**
     * 总结：
     * 1.使用proceess算子
     * 2. 定义OutputTag对象
     * 3. 调用ctx.output
     * 4.通过主流获取侧流
     */
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 1L, 1));

        /**
         * 创建OutputTag对象
         * 第一个参数：标签名
         * 第二个参数：放入侧输出流中的数据的类型.typeinfo
         */
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        //需求：将s1,s2,s3分开
        //
        SingleOutputStreamOperator<WaterSensor> processed = waterSensorDataStreamSource.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(waterSensor.getId())) {
                    /**
                     * 上下文调用output,将数据放入侧输出流
                     * 第一个参数：tag
                     * 第二个参数：放入测输出流中的数据
                     */
                    ctx.output(s1, waterSensor);
                } else if ("s2".equals(waterSensor.getId())) {
                    ctx.output(s2, waterSensor);
                } else {
                    out.collect(waterSensor);
                }
            }
        });
        //主流数据
        processed.print("主流");

        //侧输出流
        SideOutputDataStream<WaterSensor> s1DataStream = processed.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2DataStream = processed.getSideOutput(s2);

        s1DataStream.print("s1");

        s2DataStream.print("s2");
        env.execute();
    }
}
