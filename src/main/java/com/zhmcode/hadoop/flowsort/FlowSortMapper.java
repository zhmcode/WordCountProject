package com.zhmcode.hadoop.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/26.
 */
public class FlowSortMapper extends Mapper<LongWritable,Text,FlowSortBean,Text> {

    FlowSortBean flowSortBean = new FlowSortBean();
	Text phone = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] words = line.split("\t");

		String phoneNum = words[0];
		phone.set(phoneNum);

		long upFlow = Long.parseLong(words[1].trim());
		long downFlow = Long.parseLong(words[0].trim());
        flowSortBean.setFlowSortBean(upFlow,downFlow);

		context.write(flowSortBean,phone);
	}
}


