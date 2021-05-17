package com.dopedu.bigdata.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*
	 * <apple,1><apple,1><apple,1><apple,1><apple,1><apple,1><apple,1>
	 * <tea,1><tea,1><tea,1><tea,1>
	 * <hadoop,1><hadoop,1><hadoop,1><hadoop,1><hadoop,1>
	 * @reduce(KEYIN, java.lang.Iterable, Context)
	 * 输入参数：key，一组相同单词<k,v>对的key
	 */
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable value:values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}

	

}
