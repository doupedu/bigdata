package com.dopedu.bigdata.mr.fc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	//�����л�ʱ�� ��Ҫreflect���ÿղι��캯������ʽ���һ��
	public FlowBean() {}
	
	public FlowBean(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
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

	/*
	 * �����л�����
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();		
	}

	/*
	 * ���л�����
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);	
	}
	
	@Override
	public String toString(){
		return upFlow + "\t" + downFlow + "\t" + sumFlow;	
	}
}
