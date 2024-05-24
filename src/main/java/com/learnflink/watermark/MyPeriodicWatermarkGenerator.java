package com.learnflink.watermark;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPeriodicWatermarkGenerator<T> implements WatermarkGenerator<T> {
    //当前为止最大事件时间
    private long maxTs;

    //乱序等待时间
    private long delayTs;

    public MyPeriodicWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来都会调用一次，用来提取最大的时间时间保存下来
     * @param t
     * @param l
     * @param watermarkOutput
     */
    @Override
    public void onEvent(T t, long l, WatermarkOutput watermarkOutput) {

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }


}
