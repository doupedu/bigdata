package com.dopedu.bigdata.mr.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RelationJoin{
	
	static class RelationJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/*
		 * Mapper���µķ���setup()��cleanup()��
			----setup()
			�˷�����MapReduce��ܽ���ִ��һ�Σ���ִ��Map����ǰ��������ر���������Դ�ļ��г�ʼ��������
			���ǽ���Դ��ʼ���������ڷ���map()�У�����Mapper�����ڽ���ÿһ������ʱ���������Դ��ʼ�������������ظ�����������Ч�ʲ��ߣ�
			----cleanup()
			�˷�����MapReduce��ܽ���ִ��һ�Σ���ִ�����Map����󣬽�����ر�������Դ���ͷŹ�����
			���ǽ��ͷ���Դ�������뷽��map()�У�Ҳ�ᵼ ��Mapper�����ڽ���������ÿһ���ı����ͷ���Դ��
			��������һ���ı�����ǰ��Ҫ�ظ���ʼ�������·����ظ�����������Ч�ʲ��ߣ�
			���ԣ�������Դ��ʼ�����ͷŹ������ֱ���뷽��setup()��cleanup()�н���
		 */
	    private String fileName = "";
	    //todo ����new��λ��˼��������map�����ﻹ�����棿
	    private Text val = new Text();
	    private IntWritable studentKey = new IntWritable();
	 
	    protected void setup(Context context) throws java.io.IOException, InterruptedException {
	        FileSplit fileSplit = (FileSplit) context.getInputSplit();
	        fileName = fileSplit.getPath().getName();
	    };
	 
	    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
	        String[] arr = value.toString().split("\t");
	        studentKey.set(Integer.parseInt(arr[0]));
	        val.set(fileName + " " + value.toString());
	        context.write(studentKey, val);
	    };
	}
	
	static class RelationJoinReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		 
	    private Text student = new Text();
	    private Text value = new Text();
	 
	    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
	        List<String> grades = new ArrayList<String>();
	        for (Text val : values) {
	            if (val.toString().contains("student")) {
	                student.set(studentStr(val.toString()));
	            } else {
	                grades.add(gradeStr(val.toString()));
	            }
	        }
	        for (String grade : grades) {
	            value.set(student.toString() + grade);
	            context.write(value, NullWritable.get());
	        }
	    };
	 
	    //ȥ���ַ���student
	    private String studentStr(String line) {
	        String[] arr = line.split(" ");
	        StringBuilder str = new StringBuilder();
	        for (int i = 1; i < arr.length; i++) {
	            str.append(arr[i] + " ");
	        }
	        return str.toString();
	    }
	 
	    //��ȡ����
	    private String gradeStr(String line) {
	        String[] arr = line.split("\t");
	        return arr[2].toString();
	    }
	}
	
	/*
	 * 
	 * student��
	 * id	name	sex		age
	 * 1	Amy		female	18
	 * grade��
	 * id	class	grade
	 * 1	Math	89
	 * 
	 * hadoop jar RelationJoin.jar com.dopedu.bigdata.mr.join.RelationJoin /mp/in /mp/out
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length != 2) {
            throw new RuntimeException("����������·�������·��");
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("RelationJoin");
        job.setJarByClass(RelationJoin.class);
 
        job.setMapperClass(RelationJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
 
        job.setReducerClass(RelationJoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
 
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
}
