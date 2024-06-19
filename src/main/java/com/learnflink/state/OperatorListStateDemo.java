package com.learnflink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.socketTextStream("", 7777).map(new MyCountMapFunction()).print();

        env.execute();

    }

    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            return count++;
        }

        /**
         * 定义将本地变量拷贝到算子状态中，开启checkpoint时才会调用
         *
         * @param functionSnapshotContext
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

            System.out.println("Running snapshotState");
            //清空算子状态
            state.clear();
            //将本地变量添加到算子状态中
            state.add(count);
        }

        /**
         * 初始化本地变量：程序恢复时，从状态中，把数据添加到本地变量。每个子任务调用一次
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");

            //从上下文初始化算子状态
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("state", Types.LONG));
            //从算子中把数据拷贝到本地变量
            if (context.isRestored()) {
                for (Long c : state.get()) {
                    count += c;

                }
            }
        }
    }
}
