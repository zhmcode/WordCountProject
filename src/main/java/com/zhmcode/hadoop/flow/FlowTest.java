package com.zhmcode.hadoop.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


/**
 * Created by zhm on 2017/12/21.
 */
public class FlowTest {

	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowTest.class);

		// 指定job的mapper与reduce业务类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReduce.class);

		// 指定mapper输出类型的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 指定reducer输出的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 指定job的输入原生文件所在目录
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		// 指定job的输出原生文件所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
