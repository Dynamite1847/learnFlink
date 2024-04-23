package com.learnflink.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int i) {
        return Integer.parseInt(key) % i;
    }
}
