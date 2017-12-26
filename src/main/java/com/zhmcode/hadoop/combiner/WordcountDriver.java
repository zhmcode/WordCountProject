package com.zhmcode.hadoop.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zhm on 2017/12/26.
 */
public class WordcountDriver {

	public static  void  main(String [] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		conf.set("fs.defaultFS","file:///");

		Job job = Job.getInstance(conf);

		job.setJarByClass(WordcountDriver.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//指定需要使用combiner，以及用哪个类作为combiner的逻辑
//		job.setCombinerClass(WordcountCombiner.class);

//		Path inputPath = new Path("hdfs://mini1:9000/combiner/input");
//		Path outputPath = new Path("hdfs://mini1:9000/combiner/output");
		Path inputPath = new Path("D:/combiner/input");
		Path outputPath = new Path("D:/combiner/output");


	/*	FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath,true);
		}*/

		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);

		boolean b = job.waitForCompletion(true);
		if(b){
			System.exit(b?0:1);
		}
	}
}
