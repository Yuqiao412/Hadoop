package com.example.hadoopdemo.mapmatching.mapreduce.tod;

import com.example.hadoopdemo.bean.AbstractJob;
import com.example.hadoopdemo.mapmatching.mapreduce.*;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class ODCountJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
//	private Job job;

	public ODCountJob(@Qualifier("hadoopConfig") Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("TOD");

	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
//		hdfsUtils.removeDir("/tod/we");
//		Job job = Job.getInstance(config, jobName+"_tod");
//		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.0.201:9000/shanghai/we"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.0.201:9000/tod/we"));
//
//		job.setMapperClass(RecordsMap.class);
//		job.setReducerClass(ODCountReducer.class);
//		job.setGroupingComparatorClass(MyGroupingComparator.class);
//		job.setPartitionerClass(MyPartitioner.class);
//		job.setMapOutputKeyClass(MyWritable.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(3);
//		job.waitForCompletion(true);
//		return job.getStatus();
		hdfsUtils.removeDir("/todcount/we");
		Job job2 = Job.getInstance(config, jobName+"_tod_count");
		job2.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.0.201:9000/tod/we"));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.0.201:9000/todcount/we"));
		job2.setMapperClass(ODResultMap.class);
		job2.setReducerClass(ODResultReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(3);
		ODResultReducer.dayCount = 2;
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
}