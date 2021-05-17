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
 * 运行参数配置：
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
		
		//指定jar包所在的路径
		//job.setJar("/home/hadoop/wordcount.jar");
		job.setJarByClass(WordCountDriver.class);
		
		//指定本业务要使用的mapper/reducer业务类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//指定mapper输出数据的<k,v>类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//指定最终输出数据的<k,v>类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//指定job的输入原始文件，和结果输出目录
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//将job中配置的各种参数，和job所用的java类所在的jar包，提交给yarn去运行
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0:1);
	}

}
