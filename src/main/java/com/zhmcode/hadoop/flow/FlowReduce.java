package com.zhmcode.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/21.
 */
public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
		super.reduce(key, values, context);

		long sum_upFlow = 0;
		long sum_downflow =0;
		//遍历所有bean，将其中的上行流量，下行流量分别累加
		for(FlowBean item :values){
			sum_upFlow = item.getUpFlow()+sum_upFlow;
			sum_downflow = sum_downflow+item.getDownFlow();
		}
		FlowBean resultBean = new FlowBean(sum_downflow,sum_upFlow);
		context.write(key,resultBean);
	}
}
