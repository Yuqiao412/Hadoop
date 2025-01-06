package com.example.hadoopdemo.mapmatching.mapreduce.congestion.extraction;

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
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class LineResultExtractionJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
	String date = "20171210";

	public LineResultExtractionJob(Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("extraction");
	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {

		hdfsUtils.removeDir("/extraction/line/"+date);
		Job job = Job.getInstance(config, jobName+"_"+date);
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/trajectory/"+date+"/lines"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/extraction/line/"+date));

		job.setMapperClass(LineResultExtractionMap.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
//		job.waitForCompletion(true);
		job.submit();
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