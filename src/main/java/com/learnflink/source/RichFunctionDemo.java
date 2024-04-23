package com.learnflink.source;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        /**
         * RichFunction:富函数
         * 1. 多了生命周期管理方法：
         * open（）：每个子任务，在启动时，调用一次
         * close（）：每个子任务，在结束时，启动一次， 如果flink程序异常退出，不会调用close
         *
         * 2.多了一个 运行时上下文
         * 可以获取一些运行时的运行环境，比如 子任务编号，名称，其他的
         */
        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                System.out.println("SubTask Name = "+ taskNameWithSubtasks + ", SubTask Number = " + indexOfThisSubtask);
            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                System.out.println("SubTask Name = "+ taskNameWithSubtasks + ", SubTask Number = " + indexOfThisSubtask);
            }
        });

        map.print();

        env.execute();
    }
}
