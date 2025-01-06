package com.example.hadoopdemo.mapmatching.mapreduce.congestion.freeflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RoadResultMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            String[] values = value.toString().split("[,]");
            long roadID = Long.parseLong(values[1]);
            context.write(new LongWritable(roadID),  value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
