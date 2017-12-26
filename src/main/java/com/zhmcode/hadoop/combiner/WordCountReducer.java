package com.zhmcode.hadoop.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhm on 2017/12/26.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	IntWritable intWritable = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		for(IntWritable value : values){
			count+= value.get();
		}
		intWritable.set(count);
		context.write(key,intWritable);
	}
}
