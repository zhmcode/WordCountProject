package com.zhmcode.hadoop.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/26.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	Text word = new Text();
	IntWritable intWritable = new IntWritable();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String words = value.toString();
		String[] split = words.split(" ");
		for(String item : split){
			word.set(item);
			intWritable.set(1);
			context.write(word,intWritable);
		}
	}
}
