package com.learnflink.source;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.connector.datagen.source.DataGeneratorSource;

public class DataGenSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //如果有n个并行度，最大值为a
        //将数值均分成n份, a/n, 例如最大值100，并行度为2， 每个并行度生成50个
        //其中一个0-49，第二个50-99
        env.setParallelism(2);
        //数据生成器，SourceGenerator，四个参数：
        //第一个参数: GeneratorFunction接口,需要实现重写map方法,输入类型固定是Long
        //第二个参数: Long类型,自动生成的数字序列的最大值,达到最大值就停止
        //第三个参数: 限速策略,比如每秒生成几条数据
        //第四个参数：返回的数据类型

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "Number: " + value;
            }
        }, 10, RateLimiterStrategy.perSecond(1), Types.STRING);

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator").print();
        env.execute();
    }
}
