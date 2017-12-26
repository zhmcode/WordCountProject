package com.zhmcode.hadoop.flowsort;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/26.
 */
public class FlowSortReducer extends Reducer<FlowSortBean,Text,Text,FlowSortBean> {

	@Override
	protected void reduce(FlowSortBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		context.write(values.iterator().next(),bean);

	}



}
