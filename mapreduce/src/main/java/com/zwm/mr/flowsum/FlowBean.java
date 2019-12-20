package com.zwm.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化和反序列化类
 *
 * @author zhaowenming
 */
public class FlowBean implements Writable {

    /**
     * 上行流量
     */
    private long upFlow;

    /**
     * 下行流量
     */
    private long downFlow;

    /**
     * 总流量
     */
    private long sumFlow;

    /**
     * 空参构造，为了后续反射用
     */
    public FlowBean() {
        super();
    }

    /**
     * 全参数构造
     *
     * @param upFlow   上传流量
     * @param downFlow 下载流量
     */
    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    /**
     * 序列化方法
     *
     * @param out 序列化后的结果
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }


    /**
     * 反序列化方法
     *
     * @param in 需要反序列化的资源
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // 必须要求和序列化方法顺序一致
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long upFlow, long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(long upFlow, long downFlow){
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.sumFlow = upFlow + downFlow;
    }

}
