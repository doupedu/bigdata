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
	 * ���ݸ�ʽ
	 * 1860000000	c4-17-fe-ba-de-d9:CMCC	120.198.11.5	www.dopedu.com	������վ	28	23 2481	26356	200
	 * ͳ��ÿһ���ֻ������ķѵ�����������������������������
	 */
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		Configuration conf = new Configuration();
		//windows
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("yarn.resourcemanager.hostname", "hadoop-01");
		Job job = Job.getInstance(conf);
		
		//ָ��jar�����ڵ�·��
		//job.setJar("/home/hadoop/wordcount.jar");
		job.setJarByClass(FlowCount.class);
		
		//ָ����ҵ��Ҫʹ�õ�mapper/reducerҵ����
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//ָ��mapper������ݵ�<k,v>����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//ָ������������ݵ�<k,v>����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//ָ��job������ԭʼ�ļ����ͽ�����Ŀ¼
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//��job�����õĸ��ֲ�������job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0:1);
	}

	
}
