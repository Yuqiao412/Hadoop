package com.example.hadoopdemo.mapmatching.mapreduce.congestion;

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
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class CongestionJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
	String date = "20171204";

	public CongestionJob(Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("congestion");
	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {

		hdfsUtils.removeDir("/congestion/"+date);
		Job job = Job.getInstance(config, jobName+"congestion");
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/trajectory/"+date+"/lines"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/congestion/"+date));

		job.setMapperClass(LineResultMap.class);
		job.setReducerClass(CongestionReducer2ByHour.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(6);
		job.waitForCompletion(true);
		return job.getStatus();
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	//	public Job getJob() {
//		return job;
//	}
//
//	public void setJob(Job job) {
//		this.job = job;
//	}
}