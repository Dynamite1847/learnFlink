package com.learnflink.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> lineDs = env.readTextFile("C:\\Users\\dynam\\IdeaProjects\\learnFlink\\src\\main\\resources\\word.txt");

        //shuffle随机分区：random.nextInt()
//        lineDs.shuffle().print();

        //rebalance轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        //如果遇到数据源的倾斜，调用rebalance就可以解决
//        lineDs.rebalance().print();

        //rescale缩放: 实现轮询，局部组队，比热balance更高效
//        lineDs.rescale().print();

        //broadcast ：广播
//        lineDs.broadcast().print();


        //global:全局数据只发往第一个子任务
        lineDs.global().print();
        env.execute();
    }
}
