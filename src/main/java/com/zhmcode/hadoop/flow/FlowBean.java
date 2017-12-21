package com.zhmcode.hadoop.flow;


import org.apache.hadoop.io.Writable;
import org.omg.CORBA_2_3.portable.OutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/21.
 */
public class FlowBean implements Writable {

	private long downFlow;
	private long upFlow;
	private long sumFlow;

	//反序列化时，需要反射调用空参构造函数，所以要显示定义一个
	public FlowBean(){

	}
	public FlowBean(long downFlow, long upFlow) {
		this.downFlow = downFlow;
		this.upFlow = upFlow;
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
}
