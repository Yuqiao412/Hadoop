package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ShortestPathMap extends Mapper<LongWritable, Text, PairOfInts, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[,]");
        context.write(new PairOfInts(Integer.parseInt(values[2]), Integer.parseInt(values[8])), value);
    }
}
