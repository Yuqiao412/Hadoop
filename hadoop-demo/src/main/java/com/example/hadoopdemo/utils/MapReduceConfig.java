package com.example.hadoopdemo.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MapReduceConfig {
    @Value("${hadoop.hdfs}")
    private String hdfs;
    @Value("${hadoop.jobtracker}")
    private String jobtracker;
    @Value("${hadoop.resourcemanager}")
    private String resourcemanager;
    @Value("${hadoop.scheduler}")
    private String scheduler;
    @Value("${hadoop.user}")
    private String user;

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfig(){
        System.setProperty("HADOOP_USER_NAME", user);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//        conf.set("fs.default.name", hdfs);
        conf.set("fs.defaultFS", hdfs);
        conf.set("mapreduce.jobtracker.address", jobtracker);
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", resourcemanager);
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("yarn.resourcemanager.scheduler.address", scheduler);
        conf.set("mapreduce.task.timeout", "0");// no timeout
        return conf;
    }
}
