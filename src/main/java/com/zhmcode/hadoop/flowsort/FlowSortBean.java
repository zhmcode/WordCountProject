package com.zhmcode.hadoop.flowsort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/21.
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {

	public long downFlow;
	public long upFlow;
	public long sumFlow;

	//反序列化时，需要反射调用空参构造函数，所以要显示定义一个
	public FlowSortBean(){

	}
	public FlowSortBean(long downFlow, long upFlow) {
		this.downFlow = downFlow;
		this.upFlow = upFlow;
		this.sumFlow = downFlow+downFlow;
	}

	public void setFlowSortBean(long downFlow, long upFlow) {
		this.downFlow = downFlow;
		this.upFlow = upFlow;
		this.sumFlow = downFlow+downFlow;
	}


	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	/**
	 * 序列化方法
	 */
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(downFlow);
		dataOutput.writeLong(sumFlow);
	}

	/**
	 * 反序列化方法
	 * 注意：反序列化的顺序跟序列化的顺序完全一致
	 */
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public String toString() {
		return "FlowSortBean{" +
				"downFlow=" + downFlow +
				", upFlow=" + upFlow +
				", sumFlow=" + sumFlow +
				'}';
	}

	/**
	 * 排序根据Key
	 */
	public int compareTo(FlowSortBean o) {
		return this.sumFlow>o.sumFlow?-1:1;
	}
}
