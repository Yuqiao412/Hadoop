package com.example.hadoopdemo.mapmatching.mapreduce.congestion.freeflow;

import com.example.hadoopdemo.bean.AbstractJob;
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
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class FlowCountJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;

	public FlowCountJob(Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("flowcount");
	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
		hdfsUtils.removeDir("/wuhan_flowcount");
		Job job = Job.getInstance(config, jobName);
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/wuhan_trajectory/lines"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/wuhan_flowcount"));
		job.setMapperClass(RoadResultMap.class);
		job.setReducerClass(FlowCountReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
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