package com.example.hadoopdemo.mapmatching.mapreduce.Test;

import com.example.hadoopdemo.bean.AbstractJob;
import com.example.hadoopdemo.mapmatching.mapreduce.tod.ODResultMap;
import com.example.hadoopdemo.mapmatching.mapreduce.tod.ODResultReducer;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ODTestJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
//	private Job job;

	public ODTestJob(@Qualifier("hadoopConfig") Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("ODTest");

	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
		hdfsUtils.removeDir("/odtest");
		Job job = Job.getInstance(config, jobName);
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171204"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171205"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171206"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171207"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171208"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/hangzhou/20171209"));
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/SH-2018-10-10.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/shtest"));
		job.setMapperClass(ODTestMap.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);

		return job.getStatus();
	}

//	public Job getJob() {
//		return job;
//	}
//
//	public void setJob(Job job) {
//		this.job = job;
//	}
}