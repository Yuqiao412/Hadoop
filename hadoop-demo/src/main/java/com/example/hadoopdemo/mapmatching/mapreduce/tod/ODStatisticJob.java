package com.example.hadoopdemo.mapmatching.mapreduce.tod;

import com.example.hadoopdemo.bean.AbstractJob;
import com.example.hadoopdemo.mapmatching.mapreduce.MyGroupingComparator;
import com.example.hadoopdemo.mapmatching.mapreduce.MyPartitioner;
import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import com.example.hadoopdemo.mapmatching.mapreduce.RecordsMap;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ODStatisticJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
//	private Job job;

	public ODStatisticJob(@Qualifier("hadoopConfig") Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("ODStatistic");

	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
//		hdfsUtils.removeDir("/odtmp/");
//		Job job = Job.getInstance(config, jobName+"_1");
//		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171204"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171205"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171206"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171207"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171208"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171209"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171210"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/odtmp"));
//
//		job.setMapperClass(RecordsMap.class);
//		job.setReducerClass(ODStatisticReducer.class);
//		job.setGroupingComparatorClass(MyGroupingComparator.class);
//		job.setPartitionerClass(MyPartitioner.class);
//		job.setMapOutputKeyClass(MyWritable.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(6);
//		job.waitForCompletion(true);
//		return job.getStatus();
		hdfsUtils.removeDir("/ods/");
		Job job2 = Job.getInstance(config, jobName+"_2");
		job2.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
//		MultipleOutputs.addNamedOutput(job2, "ods", TextOutputFormat.class, NullWritable.class, Text.class);
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.1.11:9000/odtmp"));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.1.11:9000/ods"));
		job2.setMapperClass(ODResultMap.class);
		job2.setReducerClass(ODStatisticResultReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);

//		if(job.waitForCompletion(true) && job.getStatus().getState()==JobStatus.State.SUCCEEDED){
//			job2.waitForCompletion(true);
//			return job2.getStatus();
//		}else{
//			return job.getStatus();
//		}
		job2.waitForCompletion(true);
		return job2.getStatus();
	}
}