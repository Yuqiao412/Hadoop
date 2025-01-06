package com.example.hadoopdemo.mapmatching.mapreduce.congestion.freeflow;

import com.example.hadoopdemo.bean.AbstractJob;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.CongestionReducer;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.LineResultMap;
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
public class FreeFlowJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;

	public FreeFlowJob(Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("freeflow");
	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
//		hdfsUtils.removeDir("/freeflow_night");
//		config.setBoolean("freeflow_onlynight", false);
		hdfsUtils.removeDir("/freeflow");
		Job job = Job.getInstance(config, jobName+"_allday");
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPaths(job,
				"hdfs://192.168.1.11:9000/trajectory/20171204/lines," +
				"hdfs://192.168.1.11:9000/trajectory/20171205/lines,"+
				"hdfs://192.168.1.11:9000/trajectory/20171206/lines,"+
				"hdfs://192.168.1.11:9000/trajectory/20171207/lines,"+
				"hdfs://192.168.1.11:9000/trajectory/20171208/lines,"+
				"hdfs://192.168.1.11:9000/trajectory/20171209/lines,"+
				"hdfs://192.168.1.11:9000/trajectory/20171210/lines");
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/freeflow"));
		job.setMapperClass(RoadResultMap.class);
		job.setReducerClass(FreeFlowReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(6);
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