package com.zhmcode.hadoop.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zhm on 2017/12/18.
 */
public class WordcountTest {

	public static void main(String [] args) throws Exception {

		if (args == null || args.length == 0) {
			args = new String[2];
			args[0] = "hdfs://192.168.133.11:50070/inputformat/start-all.sh";
			args[1] = "hdfs://192.168.133.11:50070/outformat/out";
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//指定本程序的jar包所在的本地路径
		job.setJarByClass(WordcountTest.class);

		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReduce.class);

		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
