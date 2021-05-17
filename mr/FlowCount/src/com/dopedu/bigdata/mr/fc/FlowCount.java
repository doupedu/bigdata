package com.dopedu.bigdata.mr.fc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCount {
	static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
		
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String phoneCode = fields[1];
			int len = fields.length;
			Long upFlow = Long.parseLong(fields[len-3]);
			Long downFlow = Long.parseLong(fields[len-2]);
			
			context.write(new Text(phoneCode), new FlowBean(upFlow,downFlow));
		}
	}
	
	static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
		@Override
		protected void reduce(Text key,Iterable<FlowBean> values,Context context) throws IOException, InterruptedException {
			long sum_upFlow =0;
			long sum_downFlow =0;
			for(FlowBean bean:values) {
				sum_upFlow += bean.getUpFlow();
				sum_downFlow += bean.getDownFlow();
			}
			
			FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);
			context.write(key, resultBean);
		}
		
	}
	/*
	 * 数据格式
	 * 1860000000	c4-17-fe-ba-de-d9:CMCC	120.198.11.5	www.dopedu.com	教育网站	28	23 2481	26356	200
	 * 统计每一个手机号所耗费的总上行流量，下行流量，总流量
	 */
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		Configuration conf = new Configuration();
		//windows
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("yarn.resourcemanager.hostname", "hadoop-01");
		Job job = Job.getInstance(conf);
		
		//指定jar包所在的路径
		//job.setJar("/home/hadoop/wordcount.jar");
		job.setJarByClass(FlowCount.class);
		
		//指定本业务要使用的mapper/reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//指定mapper输出数据的<k,v>类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//指定最终输出数据的<k,v>类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件，和结果输出目录
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//将job中配置的各种参数，和job所用的java类所在的jar包，提交给yarn去运行
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0:1);
	}

	
}
