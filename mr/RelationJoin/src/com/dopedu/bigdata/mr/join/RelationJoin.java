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
		 * Mapper类下的方法setup()和cleanup()。
			----setup()
			此方法被MapReduce框架仅且执行一次，在执行Map任务前，进行相关变量或者资源的集中初始化工作。
			若是将资源初始化工作放在方法map()中，导致Mapper任务在解析每一行输入时都会进行资源初始化工作，导致重复，程序运行效率不高！
			----cleanup()
			此方法被MapReduce框架仅且执行一次，在执行完毕Map任务后，进行相关变量或资源的释放工作。
			若是将释放资源工作放入方法map()中，也会导 致Mapper任务在解析、处理每一行文本后释放资源，
			而且在下一行文本解析前还要重复初始化，导致反复重复，程序运行效率不高！
			所以，建议资源初始化及释放工作，分别放入方法setup()和cleanup()中进行
		 */
	    private String fileName = "";
	    //todo 关于new的位置思考，放在map函数里还是外面？
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
	 
	    //去掉字符串student
	    private String studentStr(String line) {
	        String[] arr = line.split(" ");
	        StringBuilder str = new StringBuilder();
	        for (int i = 1; i < arr.length; i++) {
	            str.append(arr[i] + " ");
	        }
	        return str.toString();
	    }
	 
	    //获取分数
	    private String gradeStr(String line) {
	        String[] arr = line.split("\t");
	        return arr[2].toString();
	    }
	}
	
	/*
	 * 
	 * student表
	 * id	name	sex		age
	 * 1	Amy		female	18
	 * grade表
	 * id	class	grade
	 * 1	Math	89
	 * 
	 * hadoop jar RelationJoin.jar com.dopedu.bigdata.mr.join.RelationJoin /mp/in /mp/out
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length != 2) {
            throw new RuntimeException("请输入输入路径、输出路径");
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
