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
public class ODJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
//	private Job job;
//    private String pathString;

	public ODJob(@Qualifier("hadoopConfig") Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("OD");
	}

//    public void setPathString(String pathString) {
//        this.pathString = pathString;
//    }

    @Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
//		hdfsUtils.removeDir("/pretreatment");
		hdfsUtils.removeDir("/od/beijing");
		Job job = Job.getInstance(config, jobName+"_pretreatment");
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/beijing"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/od/beijing"));

		MultipleOutputs.addNamedOutput(job, "od", TextOutputFormat.class, NullWritable.class, Text.class);
		job.setMapperClass(RecordsMap.class);
		job.setReducerClass(ODReducer.class);
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(MyWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
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