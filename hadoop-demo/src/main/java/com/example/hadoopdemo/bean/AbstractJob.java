package com.example.hadoopdemo.bean;

import org.apache.hadoop.mapreduce.JobStatus;

import java.io.IOException;

public abstract class AbstractJob {
    protected String jobName;
    public abstract JobStatus execute() throws IOException, ClassNotFoundException, InterruptedException;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
}
