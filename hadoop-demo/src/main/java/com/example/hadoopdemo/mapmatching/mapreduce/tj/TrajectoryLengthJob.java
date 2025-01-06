package com.example.hadoopdemo.mapmatching.mapreduce.tj;

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
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class TrajectoryLengthJob extends AbstractJob {

	private final Configuration config;
	private final HdfsUtils hdfsUtils;


	public TrajectoryLengthJob(Configuration config, HdfsUtils hdfsUtils) throws IOException {
		this.config = config;
		this.hdfsUtils = hdfsUtils;
		setJobName("trajectory");
	}


	@Override
	public JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException {
		hdfsUtils.removeDir("/pretreatment/"); // 删除第一个任务的输出文件夹 如果存在没办法运行
		Job job = Job.getInstance(config, jobName+"_pretreatment");
		job.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/tianjin/trajectory"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/pretreatment/"));

		job.setMapperClass(RecordsMap.class);
		job.setReducerClass(RecordsReducer.class);
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(MyWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);

		hdfsUtils.removeDir("/trajectory/"); // 删除第二个任务的输出文件夹 如果存在没办法运行
		Job job2 = Job.getInstance(config, jobName+"_statistics");
		job2.setJar("D:\\workspace\\bigdata\\hadoop-demo-trajectory-503\\jar\\hadoop-demo-503.jar");
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.1.11:9000/pretreatment/"));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.1.11:9000/trajectory/"));
		job2.setMapperClass(StatisticMap.class);
		job2.setReducerClass(StatisticReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job2, "dis", TextOutputFormat.class, NullWritable.class, Text.class);
		job2.setNumReduceTasks(3);

		if(job.waitForCompletion(true) && job.getStatus().getState()==JobStatus.State.SUCCEEDED){
			job2.waitForCompletion(true);
			return job2.getStatus();
		}else{
			return job.getStatus();
		}
	}
}