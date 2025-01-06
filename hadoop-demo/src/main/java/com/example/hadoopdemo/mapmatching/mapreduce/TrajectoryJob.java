package com.example.hadoopdemo.mapmatching.mapreduce;

import com.example.hadoopdemo.bean.AbstractJob;
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
public class TrajectoryJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
//	private Job job;
	private String date;

	public TrajectoryJob(Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("trajectory");
	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
//		hdfsUtils.removeDir("/pretreatment/"+date);
//		Job job = Job.getInstance(config, jobName+"_pretreatment"+date);
//		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000//hangzhou/"+date));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/pretreatment/"+date));
//
//		job.setMapperClass(RecordsMap.class);
//		job.setReducerClass(PretreatmentReducer.class);
//		job.setGroupingComparatorClass(MyGroupingComparator.class);
//		job.setPartitionerClass(MyPartitioner.class);
//		job.setMapOutputKeyClass(MyWritable.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(6);
//		job.waitForCompletion(true);
////		return job.getStatus();
		hdfsUtils.removeDir("/trajectory/"+date);
		Job job2 = Job.getInstance(config, jobName+"_restore"+date);
		job2.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.1.11:9000/pretreatment/"+date));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.1.11:9000/trajectory/"+date));
		job2.setMapperClass(RecordsMap.class);
		job2.setReducerClass(TrajectoryReducer.class);
//		job2.setReducerClass(MatchingReducer.class);
		job2.setGroupingComparatorClass(MyGroupingComparator.class);
		job2.setPartitionerClass(MyPartitioner.class);
		job2.setMapOutputKeyClass(MyWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job2, "lines", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job2, "points", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job2, "errors", TextOutputFormat.class, NullWritable.class, Text.class);
		job2.setNumReduceTasks(3);
		job2.waitForCompletion(true);
		return job2.getStatus();
//		if(job.waitForCompletion(true) && job.getStatus().getState()==JobStatus.State.SUCCEEDED){
//			job2.waitForCompletion(true);
//			return job2.getStatus();
//		}else{
//			return job.getStatus();
//		}
	}

//	public Job getJob() {
//		return job;
//	}
//
//	public void setJob(Job job) {
//		this.job = job;
//	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}