package com.learnflink.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPunctuateWatermarkGenerator<T> implements WatermarkGenerator<T> {
    //当前为止最大事件时间
    private long maxTs;

    //乱序等待时间
    private long delayTs;

    public MyPunctuateWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来都会调用一次，用来提取最大的时间时间保存下来,并发射watermark
     * @param t
     * @param l 提取到的数据的最大时间
     * @param watermarkOutput
     */
    @Override
    public void onEvent(T t, long l, WatermarkOutput watermarkOutput) {
        maxTs= Math.max(maxTs,l);
        watermarkOutput.emitWatermark(new Watermark(maxTs - delayTs -1L));
        System.out.println("调用onEvent方法， 获取目前为止最大的时间戳="+maxTs+ " ,watermark=" + (maxTs - delayTs -1L));
    }

    /**
     * 周期性调用： 发射watermark
     * @param watermarkOutput
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(maxTs - delayTs -1L));
        System.out.println("调用onPeriodicEmit方法，生成watermark=" + (maxTs - delayTs -1));
    }
}
