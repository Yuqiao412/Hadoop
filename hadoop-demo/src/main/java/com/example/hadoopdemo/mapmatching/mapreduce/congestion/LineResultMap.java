package com.example.hadoopdemo.mapmatching.mapreduce.congestion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Calendar;

public class LineResultMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            String[] values = value.toString().split("[,]");
            long lineID = Long.parseLong(values[0]);
            context.write(new LongWritable(lineID),  value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
