package com.zhmcode.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper类
 * Created by zhm on 2017/12/18.
 */
public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	/**
	 * @param key  偏移量
	 * @param value 每行的数据
	 * @param context 上下文
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
		String s = value.toString();
		if(s!=null){
			String[] words = s.split(" ");
			for(String item : words){
				context.write(new Text(item),new IntWritable(1));
			}
		}
	}
}
