package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import com.example.hadoopdemo.bean.AbstractJob;
import com.example.hadoopdemo.mapmatching.mapreduce.*;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ParallelMMJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;
	private String inputPath; // 相对路径 如"/beijing"
	private String outputPath; // 相对路径 如"/result"

	public ParallelMMJob(@Qualifier("hadoopConfig") Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("PHMM-MM");
	}

    @Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
		hdfsUtils.removeDir("/parallel/candidate/"+inputPath);
		Job job = Job.getInstance(config, jobName+"_candidate");
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path("/parallel/candidate/"+inputPath));
		job.setMapperClass(CandidateMap.class);
		job.setReducerClass(CandidateReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(12);

		hdfsUtils.removeDir("/parallel/od/"+inputPath);
		Job job2 = Job.getInstance(config, jobName+"_od");
		job2.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job2, new Path("/parallel/candidate/"+inputPath));
		FileOutputFormat.setOutputPath(job2, new Path("/parallel/od/"+inputPath));
		job2.setMapperClass(ODGroupMap.class);
		job2.setReducerClass(ODGroupReduce.class);
		job2.setGroupingComparatorClass(MyGroupingComparator.class);
		job2.setMapOutputKeyClass(MyWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(12);

		hdfsUtils.removeDir("/parallel/path/"+inputPath);
		Job job3 = Job.getInstance(config, jobName+"_path");
		job3.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job3, new Path("/parallel/od/"+inputPath));
		FileOutputFormat.setOutputPath(job3, new Path("/parallel/path/"+inputPath));
		job3.setMapperClass(ShortestPathMap.class);
		job3.setReducerClass(ShortestPathReduce.class);
		job3.setMapOutputKeyClass(PairOfInts.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setNumReduceTasks(12);

		hdfsUtils.removeDir(outputPath);
		Job job4 = Job.getInstance(config, jobName+"_mm");
		job4.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job4, new Path("/parallel/path/"+inputPath));
		FileOutputFormat.setOutputPath(job4, new Path(outputPath));
		job4.setMapperClass(HMMMap.class);
		job4.setReducerClass(HMMReduce.class);
		job4.setGroupingComparatorClass(MyGroupingComparator.class);
		job4.setMapOutputKeyClass(MyWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(NullWritable.class);
		job4.setOutputValueClass(Text.class);
		job4.setNumReduceTasks(12);
		if (job.waitForCompletion(true)){
			if(job2.waitForCompletion(true)){
				if(job3.waitForCompletion(true)){
					job4.waitForCompletion(true);
					return job4.getStatus();
				}else{
					return job3.getStatus();
				}
			}else{
				return job2.getStatus();
			}
		}else{
			return job.getStatus();
		}
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