package com.zhmcode.hadoop.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zhm on 2017/12/26.
 */
public class FlowSortDraver {

	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//指定本程序jar所在路径
		job.setJarByClass(FlowSortDraver.class);

		//指定本业务job要使用的mapper/reducer业务类
		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);

		//指定mapper输出数据的key value数据类型
		job.setMapOutputKeyClass(FlowSortBean.class);
		job.setMapOutputValueClass(Text.class);

		//指定最终输出的key value类型
		job.setOutputValueClass(FlowSortBean.class);
		job.setOutputKeyClass(Text.class);

		//指定job的输入原始文件所在目录
		Path inputPath = new Path("hdfs://mini1:9000/flowsort/input");
		Path outputPath = new Path("hdfs://mini1:9000/flowsort/output");

		//判断输出目录是否存在，如果存在就删除掉
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}

		FileInputFormat.setInputPaths(job , inputPath);
		FileOutputFormat.setOutputPath(job , outputPath);

        // 提交给yarn去运行
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
