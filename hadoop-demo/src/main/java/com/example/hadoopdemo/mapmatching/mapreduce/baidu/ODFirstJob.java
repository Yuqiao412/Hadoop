package com.example.hadoopdemo.mapmatching.mapreduce.baidu;

import com.example.hadoopdemo.bean.AbstractJob;
import com.example.hadoopdemo.mapmatching.mapreduce.MyGroupingComparator;
import com.example.hadoopdemo.mapmatching.mapreduce.MyPartitioner;
import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
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
public class ODFirstJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
	private String inputPath;
	private String outputPath;

	public ODFirstJob(@Qualifier("hadoopConfig") Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("ODFirst");
	}

	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
		hdfsUtils.removeDir(outputPath);
		Job job = Job.getInstance(config, jobName+"_"+inputPath);
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/"+inputPath));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/"+outputPath));

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

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
}