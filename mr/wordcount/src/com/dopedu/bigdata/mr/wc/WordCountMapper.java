package com.dopedu.bigdata.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 * KEYIN:默认是mapreduce框架所读到的第一行文本的偏移量,Long;在hadoop中使用自己的精简的序列化接口LongWritable
 * VALUEIN:默认是mapreduce框架所读到的第一行文本,String
 * KEYOUT:用户自定义逻辑处理完成后输出数据中的key,String
 * VALUEOUT:用户自定义逻辑处理完成后输出数据中的value,Integer
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 * map阶段业务默认写在自定义的map()方法中
	 * maptask会对每一行的输入数据调用一次自定义的map()方法
	 * map(KEYIN, VALUEIN, Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		String line =value.toString();
		String[] words = line.split(" ");
		
		//将单词输出为 <单词,1>
		for(String word:words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
	
}
