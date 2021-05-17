package com.dopedu.bigdata.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ���в������ã�
 * java -cp wordcount.jar com.dopedu.bigdata.mr.wc.WordCountDriver /wordcount/input /wordcount/output
 * hadoop jar wordcount.jar com.dopedu.bigdata.mr.wc.WordCountDriver /wordcount/input /wordcount/output
 */
public class WordCountDriver {
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		Configuration conf = new Configuration();
		//windows
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("yarn.resourcemanager.hostname", "hadoop-01");
		Job job = Job.getInstance(conf);
		
		//ָ��jar�����ڵ�·��
		//job.setJar("/home/hadoop/wordcount.jar");
		job.setJarByClass(WordCountDriver.class);
		
		//ָ����ҵ��Ҫʹ�õ�mapper/reducerҵ����
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//ָ��mapper������ݵ�<k,v>����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//ָ������������ݵ�<k,v>����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//ָ��job������ԭʼ�ļ����ͽ�����Ŀ¼
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//��job�����õĸ��ֲ�������job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0:1);
	}

}
