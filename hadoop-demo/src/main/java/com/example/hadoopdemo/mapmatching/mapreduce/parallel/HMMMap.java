package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import ch.hsr.geohash.GeoHash;
import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HMMMap extends Mapper<LongWritable, Text, MyWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[,]");
        context.write(new MyWritable(values[0], Long.parseLong(values[1])), value);
    }
}
