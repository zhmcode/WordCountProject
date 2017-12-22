package com.zhmcode.hadoop.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/21.
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//将一行内容转成string
		String line = value.toString();
		//切分字段
		String[] fields = line.split("\t");
		//取出手机号
		String phoneNbr = fields[1];
		//取出上行流量下行流量
		long upFlow = Long.parseLong(fields[fields.length-3]);
		long downFlow = Long.parseLong(fields[fields.length-2]);
        context.write(new Text(phoneNbr),new FlowBean(downFlow,upFlow));

	}
}
