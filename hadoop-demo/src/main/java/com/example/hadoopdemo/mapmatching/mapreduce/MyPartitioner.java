package com.example.hadoopdemo.mapmatching.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyWritable, Text> {

    @Override
    public int getPartition(MyWritable key, Text value, int numReduceTasks) {

        return (key.getVehicleID().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
