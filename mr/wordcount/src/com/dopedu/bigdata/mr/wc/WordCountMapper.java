package com.dopedu.bigdata.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 * KEYIN:Ĭ����mapreduce����������ĵ�һ���ı���ƫ����,Long;��hadoop��ʹ���Լ��ľ�������л��ӿ�LongWritable
 * VALUEIN:Ĭ����mapreduce����������ĵ�һ���ı�,String
 * KEYOUT:�û��Զ����߼�������ɺ���������е�key,String
 * VALUEOUT:�û��Զ����߼�������ɺ���������е�value,Integer
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 * map�׶�ҵ��Ĭ��д���Զ����map()������
	 * maptask���ÿһ�е��������ݵ���һ���Զ����map()����
	 * map(KEYIN, VALUEIN, Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		String line =value.toString();
		String[] words = line.split(" ");
		
		//���������Ϊ <����,1>
		for(String word:words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
	
}
